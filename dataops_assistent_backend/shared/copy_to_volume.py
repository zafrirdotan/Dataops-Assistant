import os
import tarfile
import io
import docker
import logging
from contextlib import suppress

def copy_to_volume(volume_name, source_path, dest_path="/dst", helper_image="alpine:3.20"):
    """
    Copy a file or directory from the local filesystem into a Docker volume using a helper container.
    Args:
        volume_name (str): Name of the Docker volume.
        source_path (str): Local file or directory to copy.
        dest_path (str): Destination path inside the volume (default: /dst).
        helper_image (str): Helper image to use (default: alpine:3.20).
    Raises:
        Exception: On failure to copy files.
    """
    client = docker.from_env()
    logger = logging.getLogger("copy_to_volume")
    logger.setLevel(logging.INFO)

    # Ensure the volume exists
    try:
        client.volumes.get(volume_name)
    except docker.errors.NotFound:
        client.volumes.create(name=volume_name)

    # Prepare tar archive in memory
    tarstream = io.BytesIO()
    with tarfile.open(fileobj=tarstream, mode='w') as tar:
        if os.path.isdir(source_path):
            for root, dirs, files in os.walk(source_path):
                for file in files:
                    fullpath = os.path.join(root, file)
                    arcname = os.path.relpath(fullpath, start=source_path)
                    tar.add(fullpath, arcname=arcname)
        else:
            tar.add(source_path, arcname=os.path.basename(source_path))
    tarstream.seek(0)

    # Helper function to create and start the container
    def start_helper():
        return client.containers.create(
            image=helper_image,
            command="sleep 10",
            volumes={volume_name: {"bind": dest_path, "mode": "rw"}},
            detach=True
        )

    helper = None
    try:
        try:
            helper = start_helper()
        except docker.errors.ImageNotFound:
            logger.info(f"Pulling helper image {helper_image}...")
            client.images.pull(helper_image)
            helper = start_helper()
        helper.start()
        helper.put_archive(dest_path, tarstream.read())
        logger.debug(f"Copied {source_path} to volume {volume_name}:{dest_path}")
    except Exception as e:
        logger.error(f"Failed to copy to volume: {e}")
        raise
    finally:
        if helper is not None:
            with suppress(Exception):
                helper.stop()
            with suppress(Exception):
                helper.remove(force=True)

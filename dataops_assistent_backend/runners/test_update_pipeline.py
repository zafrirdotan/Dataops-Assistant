from pipeline_builder.registry.pipeline_registry_service import pipelineRegistryService

async def test_update_pipeline():
    pipeline_id = "test_pipeline_123"
   
    await pipelineRegistryService.create_pipeline(
            pipeline_id=pipeline_id,
            name="Test Pipeline",
            created_by="tester",
            description="A pipeline for testing update.",
            spec={"test": True}
        )
    print(f"Created pipeline with ID: {pipeline_id}")
    # Now, update the image_id
    updated = await pipelineRegistryService.update_pipeline(pipeline_id, {"image_id": "test_image_456"})

    print(f"Updated pipeline: {updated.pipeline_id}, image_id: {updated.image_id}")

if __name__ == "__main__":
    test_update_pipeline()
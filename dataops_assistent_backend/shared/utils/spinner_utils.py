import shutil
from yaspin import yaspin

async def run_step_with_spinner(step_msg, step_number, coro, *args, status_col=65, color="cyan", **kwargs):
    """
    Run an async step with a CLI spinner, aligning the status to a fixed column.
    Returns (result, error). If error is not None, result is None.
    """
    # Get terminal width, default to 80 if unavailable
    try:
        terminal_width = shutil.get_terminal_size().columns
    except (OSError, AttributeError):
        terminal_width = 80

    # Adjust status column to fit terminal width (leave room for spinner, colors, and status text)
    # Reserve ~25 chars for spinner, colors, and "Success"/"Failed" text
    max_status_col = max(20, terminal_width - 25)
    status_col = min(status_col, max_status_col)

    text = f" Step {step_number}: {step_msg}"  # Add leading space to shift spinner right
    # Truncate text if it's too long for the available space
    if len(text) > status_col:
        text = text[:status_col-3] + "..."
    padded_text = text.ljust(status_col)

    spinner = yaspin(text=text, color=color)
    spinner.start()
    try:
        result = await coro(*args, **kwargs)
        spinner.text = ''
        try:
            spinner.ok(f"\033[92m✔\033[0m {padded_text}\033[92mSuccess\033[0m")
        except ValueError as spinner_err:
            # Fallback if terminal is still too small
            spinner.stop()
            print(f"✓ Step {step_number}: {step_msg} - Success")
        return result, None
    except Exception as e:
        spinner.text = ''
        try:
            spinner.fail(f"\033[91m✖\033[0m {padded_text}\033[91mFailed\033[0m")
        except ValueError as spinner_err:
            # Fallback if terminal is still too small
            spinner.stop()
            print(f"✗ Step {step_number}: {step_msg} - Failed: {e}")
        return None, e

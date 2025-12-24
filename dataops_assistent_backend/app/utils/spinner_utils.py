import asyncio
from yaspin import yaspin

async def run_step_with_spinner(step_msg, step_number, coro, *args, status_col=65, color="cyan", **kwargs):
    """
    Run an async step with a CLI spinner, aligning the status to a fixed column.
    Returns (result, error). If error is not None, result is None.
    """
    text = f" Step {step_number}: {step_msg}"  # Add leading space to shift spinner right
    padded_text = text.ljust(status_col)
    spinner = yaspin(text=text, color=color)
    spinner.start()
    try:
        result = await coro(*args, **kwargs)
        spinner.text = ''
        spinner.ok(f"\033[92m✔\033[0m {padded_text}\033[92mSuccess\033[0m")
        return result, None
    except Exception as e:
        spinner.text = ''
        spinner.fail(f"\033[91m✖\033[0m {padded_text}\033[91mFailed\033[0m")
        return None, e

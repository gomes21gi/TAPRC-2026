import datetime
import logging
import os

import azure.functions as func


app = func.FunctionApp()


@app.timer_trigger(
    schedule=os.getenv("TIMER_SCHEDULE", "0 */5 * * * *"),
    arg_name="timer",
    run_on_startup=False,
    use_monitor=True,
)
def timer_trigger(timer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat()
    logging.info("Timer Trigger executado em %s", utc_timestamp)

    if timer.past_due:
        logging.warning("O Timer Trigger esta atrasado.")
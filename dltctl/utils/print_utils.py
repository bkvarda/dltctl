import click
import datetime

def event_print(type, level, msg):
    ts = datetime.datetime.utcnow().isoformat()[:-3]+'Z'
    color = 'red' if level == 'ERROR' else 'green'
    emoji = u'\u2714'
    click.secho(emoji + " ", fg=color, nl=False)
    click.secho(ts + " ", nl=False)
    click.secho(type + " ", nl=False, fg=color)
    click.secho(msg)
    return
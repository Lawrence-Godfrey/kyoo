# kyoo

A sidebar for [kyoo](https://github.com/Lawrence-Godfrey/kyoo), a local job queue for long-running and GPU-bound commands that you and your coding agent share. Jobs take named locks (`gpu0`, `dvc`) so they never collide, can wait on other jobs, and keep running when the agent session ends.

The sidebar shows what is running and what is waiting. A waiting job names the job it is waiting on. Expand a job to see when it was queued, how long it ran, its locks, what it depends on and what depends on it. Its command and log open in the editor, and active jobs can be cancelled from the list. The status bar shows the running and waiting counts.

## Setup

On the machine that runs the jobs:

```
claude mcp add kyoo -- uvx kyoo mcp
```

Then install this extension. The agent queues jobs through the MCP server, which starts the kyoo daemon on first use. If the daemon is not running, the sidebar offers to start it. That needs [uv](https://docs.astral.sh/uv/).

## Remote SSH

The extension runs on the remote host, next to the daemon. Install it while connected, and VS Code puts it on the remote side.

# kyoo

A sidebar for [kyoo](https://github.com/Lawrence-Godfrey/kyoo), a local job queue for long-running and GPU-bound commands that you and your coding agent share. Jobs take named locks (`gpu0`, `dvc`) so they never collide, can wait on other jobs, and keep running when the agent session ends.

The sidebar shows what is running and what is waiting. A waiting job names the job it is waiting on. Expand a job to see when it was queued, how long it ran, its locks, what it depends on and what depends on it. Its command and log open in the editor, and active jobs can be cancelled from the list. The status bar shows the running and waiting counts.

![The kyoo sidebar: a training job running on the GPU and an evaluation waiting on it](https://raw.githubusercontent.com/Lawrence-Godfrey/kyoo/main/docs/images/vscode-sidebar.png)

Your agent queues work through kyoo's MCP server. Here Claude Code queues a training run that holds the `gpu0` lock, then an evaluation that starts only if training succeeds:

![Claude Code calling enqueue for a training job and an evaluation that depends on it](https://raw.githubusercontent.com/Lawrence-Godfrey/kyoo/main/docs/images/agent-enqueue.png)

It then waits on both with `wait_job`, which returns each job's status and result without the agent polling:

![Claude Code calling wait_job on both jobs, which return succeeded](https://raw.githubusercontent.com/Lawrence-Godfrey/kyoo/main/docs/images/agent-wait.png)

## Setup

On the machine that runs the jobs:

```
claude mcp add kyoo -- uvx kyoo mcp
```

Then install this extension. The agent queues jobs through the MCP server, which starts the kyoo daemon on first use. If the daemon is not running, the sidebar offers to start it. That needs [uv](https://docs.astral.sh/uv/).

## Remote SSH

The extension runs on the remote host, next to the daemon. Install it while connected, and VS Code puts it on the remote side.

# rota

A local job queue for agents and people. Queue a command with the resources it needs and the jobs it must wait for; rota runs it when they are free, keeps the log, and records the result. Check on it from the command line, from an MCP server, or from a web page.

One Python package. State is a SQLite file in your user data directory, the daemon starts itself the first time a client connects, and nothing else is installed.

## Install

```
claude mcp add --scope user rota -- uvx rota mcp   # agents: register the MCP server
uvx rota ui                                        # people: print and open the web page
uvx rota install-service                           # optional: a systemd user service, so the daemon survives logouts and reboots
```

Without `uv`: `pipx install rota`, then `rota mcp`, `rota ui`, and so on.

## Use

```
rota add "uv run train.py --params /jobs/a/params.yaml" --lock gpu0 \
    --description "KL 0.1, width-matched components" \
    --preflight "python -m mytracking check" --result /jobs/a/result.json
rota add "dvc repro" --cwd ~/wt/masking --lock gpu0 --lock worktree:masking --after 12
rota ls --status queued,held,running
rota get 13 --tail 20
rota wait 13
rota cancel 14
rota lock cpu 4
```

- **locks**: names of things a job needs to itself. Two jobs holding the same name never run at once; `rota lock <name> <n>` lets `n` run. Names are yours: `gpu0`, `worktree:masking`, `db:staging`.
- **after**: job ids that must succeed first. If one of them fails or is cancelled, the job is skipped rather than run.
- **preflight**: a command that must exit 0 just before the job starts. If it fails the job is held and retried a minute later, up to ten times, with the output in the job's error field. Use it for things that can be fixed while the job waits: an expired login, a missing file.
- **result**: a JSON file the job writes. Its contents are stored with the job when it finishes, so a run's final numbers are one `rota get` away.
- **urgent** (`--urgent`): run before other queued jobs once the locks allow.

Jobs run as the user who runs the daemon, in the working directory given at enqueue time, with the daemon's environment plus any `--env KEY=VALUE`. Output goes to `logs/<id>.log` in the data directory. Cancelling a running job sends SIGTERM to its process group, then SIGKILL after ten seconds.

## MCP tools

`enqueue`, `list_jobs`, `get_job` (with an optional log tail), `wait_job`, `tail_log`, `cancel_job`, `locks`, `set_lock_capacity`. Responses are compact: a job is its id, status, description, command, locks, dependencies, exit code, error and result.

## Web page

`rota ui` prints `http://127.0.0.1:7777/?token=…`. The page lists jobs with their status, age and duration, shows lock usage, and opens a job's details and log on click. It refreshes every two seconds. The token in the URL is read once into session storage, so bookmark the printed URL.

## How it works

`rota serve` is the daemon: a scheduler thread and an HTTP API on `127.0.0.1:7777`. Every half second it reaps finished processes, then walks the queue in priority order and starts each job whose dependencies have succeeded, whose locks are free and whose preflight passes. The API requires a bearer token stored in the data directory with owner-only permissions, so other users on the machine cannot read or add jobs. Clients (`rota`, the MCP server, the page) start the daemon if nothing answers.

Data directory: `~/.local/share/rota` on Linux (`$XDG_DATA_HOME/rota` if set), `~/Library/Application Support/rota` on macOS, or `$ROTA_HOME`. `ROTA_PORT` changes the port.

## Development

```
uv sync --all-groups
uv run pytest           # end-to-end tests against a real daemon in a temp directory
uv run ruff check src tests
```

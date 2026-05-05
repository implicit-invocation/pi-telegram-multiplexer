# pi-telegram-multiplexer

A pi package that lets one Telegram bot control multiple pi instances across multiple workspaces.

## Install

```bash
pi install git:github.com/your-user/pi-telegram-multiplexer
# or from a local checkout
pi install /path/to/pi-telegram-multiplexer
```

## First run

On the first pi startup after installation, the extension opens a wizard:

1. Paste your Telegram bot token.
2. Choose the workspaces root: current folder, parent folder, or a typed path.

If you leave the token empty, setup is marked as skipped and you will not be prompted again. Run this anytime to configure or reconfigure:

```text
/telemulti-setup
```

Config files live under `~/.pi/agent/`:

- `telemulti.json` - bot token and workspaces root
- `telemulti-server.json` - current local websocket server pointer
- `telemulti-state.json` - approved users, pending users, chat/workspace bindings

## Local server

After setup, each pi instance connects to a local websocket server. If no healthy server pointer exists, the extension starts `server.mjs` and connects to it. The server owns Telegram polling and routes messages to connected pi instances.

The extension reconnects with backoff if the websocket closes, and starts a fresh server if the stored pointer is stale. The server pings clients, removes stale clients, restarts long-stalled Telegram polling, and removes its pointer on shutdown.

## Commands inside pi

```text
/telemulti-setup       Run setup wizard again
/telemulti-status      Show status
/telemulti-disconnect  Disconnect this instance from the server
/telemulti-reset       Clear settings, approvals, and chat/workspace connections
/telemulti-pending     Open a TUI picker to approve/reject pending Telegram users
/telemulti-approve ID  Approve a Telegram user id directly
/telemulti-reject ID   Reject a Telegram user id directly
```

## Telegram usage

1. Send `/start` to the bot. The bot replies `pending approval` until approved from any connected pi instance.
2. After approval, use:

```text
/workspaces
/connect <workspace-path>
```

`/workspaces` shows active workspaces with inline **Connect** buttons. Tapping a button connects the current chat to that workspace.

`/connect` accepts an active absolute workspace, a path relative to the configured workspaces root, or a new path. If the folder does not exist, the bot asks you to confirm with `/confirm <id>` before creating it and starting pi.

Once a chat is connected to a workspace, text and attachments sent to that chat are forwarded to the pi instance in that workspace. Pi replies and files queued with `telemulti_attach` are broadcast to all Telegram chats connected to the same workspace.

## Notes

- Spawning uses the `pi` command by default. Override with `TELEMULTI_PI_COMMAND=/path/to/pi` if needed.
- Spawned workspaces use `pi --mode rpc -e <this package>` so they can run headlessly while the extension receives work through the websocket server.
- Telegram files are downloaded to `~/.pi/agent/tmp/telemulti` and passed to pi as local file paths. Images are also included as image inputs.

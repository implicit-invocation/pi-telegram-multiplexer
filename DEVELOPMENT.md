# Local Development

## Prerequisites

- Node.js 22+ recommended.
- `pi` installed and available on `PATH`.
- A Telegram bot token from [@BotFather](https://t.me/BotFather).

## Install dependencies

```bash
npm install
```

## Type-check

```bash
npm run check
```

## Syntax-check the local server

```bash
node --check server.mjs
```

## Run the extension from this checkout

From any workspace where you want to test the extension:

```bash
pi -e /Users/dev/projects/pi-telegram-multiplexer
```

On first run, the setup wizard asks for:

1. Telegram bot token.
2. Workspaces folder: current folder, parent folder, or a custom path.

If you skip setup, run it later inside pi:

```text
/telemulti-setup
```

## Useful pi commands while testing

```text
/telemulti-status       Show connection/config status
/telemulti-pending      Open a TUI picker to approve/reject pending Telegram users
/telemulti-approve ID   Approve a Telegram user directly
/telemulti-reject ID    Reject a Telegram user directly
/telemulti-disconnect   Disconnect this pi instance from the local server
/telemulti-reset        Clear settings, approvals, and chat/workspace connections
```

## Telegram test flow

1. Start pi with the local extension path.
2. Complete `/telemulti-setup`.
3. In Telegram, send `/start` to the bot.
4. In pi, run `/telemulti-pending`.
5. Pick your Telegram user in the TUI, then choose `approve` or `reject`.
6. In Telegram, run:

```text
/workspaces
```

Tap a workspace's **Connect** button, or manually run:

```text
/connect <workspace-path>
```

After a chat is connected, normal Telegram messages and attachments are forwarded to the pi instance for that workspace.

## Local state files

The extension writes local runtime/config state under `~/.pi/agent/`:

- `telemulti.json` - bot token, bot metadata, and configured workspaces root.
- `telemulti-server.json` - active local websocket server pointer.
- `telemulti-state.json` - approved users, pending users, and chat/workspace bindings.
- `tmp/telemulti/` - downloaded Telegram files.

To reset local development state:

```bash
rm -f ~/.pi/agent/telemulti.json \
      ~/.pi/agent/telemulti-server.json \
      ~/.pi/agent/telemulti-state.json
rm -rf ~/.pi/agent/tmp/telemulti
```

If a stale server process is still running, find and stop it:

```bash
ps aux | grep 'server.mjs' | grep pi-telegram-multiplexer
kill <pid>
```

## Testing spawned workspace instances

When Telegram `/connect` targets a workspace without an active connected pi instance, the server spawns one using:

```bash
pi --mode rpc -e /Users/dev/projects/pi-telegram-multiplexer
```

Override the command used for spawning if needed:

```bash
TELEMULTI_PI_COMMAND=/absolute/path/to/pi pi -e /Users/dev/projects/pi-telegram-multiplexer
```

## Before committing

Run:

```bash
npm run check
node --check server.mjs
git status --short
```

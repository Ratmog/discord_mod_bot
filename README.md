# ModBot

A Discord moderation bot for a server where edgy humor is normal but a few things are not:

- **Private reports.** `/report <message link>` or right-click a message → Apps → **Report to ModBot**. Replies are visible only to the reporter, and reporter identity is never stored (only an HMAC, for the cooldown).
- **Auto dry-run.** Every human message is scored with OpenAI Moderation (free). Rules start unenforced and only log what they *would* do, so thresholds can be calibrated on real traffic before anything is deleted.
- **Child safety, live from day one.** `sexual/minors` content is deleted on OpenAI's own flag even in dry-run, the text is never stored, and moderators are pinged.
- **Game-bot cleanup.** Pokétwo and Lawliet messages are deleted 5–6 minutes after they post, surviving restarts.

The full design, gates, and Phase 2 (local ML model) plan live in the technical plan doc.

## How decisions work

```
message ──► scores (cached per message + content + model) ──► evaluate() against
            current rules.yaml + current enforce/dry-run settings ──► DELETE / FLAG / KEEP
```

Only the expensive classifier call is cached; decisions are recomputed for every trigger. So:

- A **report** re-judges an already-scored message with the stricter `report` threshold, no second API call.
- Turning a rule on with `/modbot enforce` takes effect immediately, even for messages scored earlier.
- Only categories listed in `rules.yaml` can ever cause a deletion. A report lowers the bar for those; it never makes ordinary offensiveness deletable.

## Setup

1. **Create the app** at <https://discord.com/developers/applications> → Bot. Turn on **Message Content Intent**. Copy the token.
2. **Invite it** with scopes `bot` + `applications.commands` and permissions: View Channels, Read Message History, Send Messages, Embed Links, Manage Messages.
3. **Create a private `#mod-logs` channel** the bot can post in, and (optionally) a moderator role to ping.
4. **Configure:**
   ```bash
   python3.11 -m venv venv && . venv/bin/activate
   pip install -r requirements.txt
   cp .env.example .env   # then fill it in
   ```
   Get IDs with Discord's Developer Mode (Settings → Advanced), then right-click → Copy ID. Copy the game bots' user IDs from your own server.
5. **Run:** `python -m modbot`

Commands sync to your guild on startup, so they appear immediately.

## Admin commands (administrators only)

| Command | What it does |
| --- | --- |
| `/modbot dryrun on\|off` | Global override. Child safety ignores it. |
| `/modbot enforce <rule> on\|off` | Turn deletion on for one rule. The only writer of enforce state after first start. |
| `/modbot stats` | Cases by rule, decision and trigger for the last 7 days, plus queue health. |
| `/modbot recheck <link>` | Re-score a message using report thresholds. |

Each `#mod-logs` post has **Violation / Fine** buttons. Marking cases is optional, but those marks are the calibration data for picking real thresholds.

## Rollout

1. Run in a test server until reporting and cleanup work across a restart (gates G1, G2).
2. Add to the real server. Cleanup and child safety are live; everything else is dry-run.
3. After ≥ 2 weeks and ≥ 30 reviewed would-deletes for a rule, tune its thresholds in `rules.yaml` and `/modbot enforce` it. Some rules may never get there; they stay flag-and-keep, which is fine.

## Configuration notes

- `CLEANUP_INCLUDE_COMMANDS=true` also deletes members' messages that mention or reply to a game bot (e.g. `@Pokétwo c pikachu`). Off by default.
- `rules.yaml` values are illustrative until calibrated. Editing it changes `rules_version`, recorded on every case.
- `DRY_RUN` and each rule's `default_enforce` only **seed** the database on first start; afterwards the `/modbot` commands are authoritative.

## Development

```bash
pip install -r requirements-dev.txt
pytest -q && ruff check .
```

The core (`rules`, `pipeline`, `cleanup`, `db`) never imports discord.py directly; tests use a fake classifier and a fake gateway and run offline.

## Deploying (Oracle Always Free ARM)

Copy the repo to `/opt/modbot`, create the venv there, put `.env` in place (mode 600), then:

```bash
sudo cp deploy/modbot.service /etc/systemd/system/
sudo systemctl enable --now modbot
journalctl -u modbot -f
```

Upgrade the Oracle tenancy to Pay-As-You-Go (Always Free resources stay $0) so the VM isn't reclaimed for being idle, and back up `modbot.db` nightly.

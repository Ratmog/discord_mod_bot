# ModBot

A Discord moderation bot for a server where edgy humor is normal but a few things are not:

- **Private reports.** `/report <message link>` or right-click a message → Apps → **Report to ModBot**. Replies are visible only to the reporter, and reporter identity is never stored (only an HMAC, for the cooldown).
- **Auto dry-run.** Every human message is scored (including messages sent through proxy bots like PluralKit, which post via webhooks) with OpenAI Moderation (free). Rules start unenforced and only log what they *would* do, so thresholds can be calibrated on real traffic before anything is deleted.
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
3. **Create a private `#mod-logs` channel** the bot can post in, and (optionally) a moderator role to ping. Make that role **mentionable**, or give ModBot "Mention @everyone, @here, and All Roles"; otherwise pings show up but notify nobody. ModBot checks this on startup and posts any setup problems to `#mod-logs`.
4. **Configure:**
   ```bash
   python3.11 -m venv venv && . venv/bin/activate
   pip install -r requirements.txt   # needs discord.py 2.5+
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
| `/modbot recheck <link>` | Re-judge a message against current rules with report thresholds. Deletes it if that rule is enforced. |

Each `#mod-logs` post has **Violation / Fine** buttons. Marking cases is optional, but those marks are the calibration data for picking real thresholds.

## Reliability guarantees

- **One action per message.** Concurrent reports of the same message cause exactly one Discord delete.
- **Audit before action.** An enforced delete is recorded as `pending` before Discord is called. A recovery pass runs at startup and every 2 minutes: a message that's still up is re-judged and removed if it still breaks a rule; one that's gone is recorded as `gone_unconfirmed`, since ModBot can't prove it was the one that deleted it. If the re-check itself can't run (OpenAI down and the text was edited meanwhile), the original alert is kept and posted so a moderator looks at it. Recovery never touches a delete that's still in flight, and a resolved case is never overwritten.
- **Mod-log posts can't be lost.** Each post is queued with its case and retried every minute until Discord accepts it, so a crash or outage can't silently drop a child-safety alert. (A crash at exactly the wrong moment can repeat a post; it can't drop one.)
- **Outages are retried.** Messages that arrive while OpenAI is down are re-scored up to 3 times with growing delays. These retries live in memory, so a restart during an outage drops them.
- **Missed game messages.** After a restart or reconnect, recent channel history is scanned for game-bot messages the bot never saw. If any channel fails, the scan's starting point is kept and the scan retries next minute.

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

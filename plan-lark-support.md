## Plan: Multi-Channel WeChat and Lark

Add multiple Feishu/Lark bot accounts as peer interaction adapters beside WeChat, all feeding one shared `TaskManager`, global `AgentRegistry`, durable store, and process-isolated Agents. Each bot is added by the local OS owner through a terminal QR scan using a version-pinned `lark-cli`, receives an isolated credential/config directory and supervised connection, and can list or switch to any enabled Agent. Active Agent routes and conversational state remain local to each bot/account and chat; administrator-configured canonical principals are used for authorization and optional person-level policy without redirecting replies or synchronizing Agent selection across bots.

**Steps**

### Phase 1: Make shared control-plane boundaries explicit
1. Update the architecture documentation to describe channels as peer adapters around one shared runtime, correct the documented schema version drift, and define the distinction among transport account, canonical principal, conversation subject, Agent, and outbound reply target.
2. Extract the common command registry/router from `src/channels/wechat.py` into a channel-neutral module. Keep parsing and runtime command effects shared, while leaving WeChat-only rendering, `/recv`, typing, reply quota, and iLink metadata in the WeChat adapter.
3. Remove unsafe channel defaults from normalized ingress models or require adapters to set `channel` explicitly. Add channel capability/delivery-policy contracts so WeChat keeps its current 10-send/3,000-character behavior and Lark can use its own limits, threading, and attachment capabilities.
4. Separate generic logical delivery fields from iLink-specific wire metadata in `UserDelivery`, `DeliveryReceipt`, runtime delivery records, and store APIs. Preserve current WeChat behavior behind a WeChat projection/metadata codec before adding Lark.

### Phase 2: Add canonical principal identity
5. Add principal-domain types and a `PrincipalResolver` that maps only authenticated `(channel, bot_id, external_user_id)` values to an administrator-configured `principal_id`. Never accept a principal ID from event payload text, raw metadata, attachments, or commands.
6. Add owner-only bot-profile and identity configuration. Every Lark bot profile has a local profile ID, stable app ID used as `bot_id`, `feishu`/`lark` brand, isolated `LARKSUITE_CLI_CONFIG_DIR`, enabled state, mention/access policy, restart policy, and optional principal mappings. Reject duplicate live app IDs, reused config directories, unstable user identifiers, insecure state permissions, and unsupported `lark-cli` versions.
7. Append a new SQLite migration after the actual latest schema version. Add durable Lark bot-profile metadata, account health/onboarding state, `principals`, and `principal_accounts`; snapshot principal and conversation-subject provenance on new work while retaining transport keys unchanged for dedupe, command receipts, routes, conversations, reply scopes, outboxes, media, cursors, and delivery leases.
8. Keep active Agent routes, mode, model, role, working-directory, session controls, and provider conversations scoped by `(channel, bot_id, external_user_id or conversation_subject, session_id)`. Thus every bot can access the global Agent catalog, but `/agent` on Bot A does not switch Bot B or WeChat. Use principal identity for authorization and optional person-level policy only, not as the default routing or conversation key.
9. Introduce conversation subjects separate from actors: Lark direct messages use the bot-local sender account, group roots use the bot-local chat identity, and topic threads use bot plus chat plus thread/root identity. Authorization uses the resolved actor principal; memory continuity and routing use the bot-local conversation subject. Preserve legacy conversation IDs and immutable historical tasks.
10. Backfill existing accounts without changing their route or conversation behavior. Administrator mappings may associate identities for authorization/audit, but historical provider threads and bot-local state are never merged automatically.

### Phase 3: Add terminal QR onboarding and the Lark CLI adapter
11. Add an owner-only `./cow lark add [profile-name]` command. It creates a private temporary/config directory, runs a version-pinned `lark-cli config init --new`, relays its terminal QR code and verification URL, waits for the scan, verifies the returned app identity and brand, persists only the resulting CLI profile/secret reference, and atomically registers the bot profile. Cancellation, expiration, duplicate app IDs, malformed output, and partial credential persistence must roll back cleanly.
12. Add `./cow lark list`, `status`, `reauthorize`, `disable`, `enable`, and `remove` commands. Reauthorization repeats the PersonalAgent registration/binding scan for that profile; it is distinct from `lark-cli auth login`, which is excluded unless later user-delegated APIs require OAuth. Bot connections must restart unattended from stored app credentials after supervisor or machine restart.
13. Add `src/channels/lark.py` with one account supervisor per registered bot, each using an isolated `LARKSUITE_CLI_CONFIG_DIR`, generation-fenced `LarkCliProcess`, event normalization, gateway, attachment promoter, text delivery worker, and media delivery worker. Pin and contract-test readiness markers, QR/init output parsing, NDJSON events, message dedupe keys, send/reply acknowledgements, errors, termination, and restart behavior.
14. Run one long-lived `event consume im.message.receive_v1 --as bot` process per bot profile, plus additional account-local consumers only for required event keys. Normalize every event with `channel="lark"` and the configured app ID as `bot_id`; verify the event belongs to that profile before acceptance. Use tenant-stable sender `open_id`, immutable message ID, exact chat/thread reply target, timestamps, structured mentions, and sanitized raw metadata.
15. Apply intake policy before durable acceptance: direct chats accept ordinary prompts and commands; group chats require a structured mention of that exact bot; strip only the authenticated mention entity, never display-name text. `/agents` lists the shared global Agent catalog and `/agent <id>` changes only that bot-local user/chat route. Ordinary users may switch among existing enabled Agents; dynamic Agent creation/profile selection/deletion requires administrator authority because those mutate the global registry visible to every bot.
16. Implement basic inbound/outbound images and files through `AttachmentStore` and account-specific upload/download checkpoints. Keep transport secrets and expiring download tokens out of logs and generic durable metadata.
17. Implement one text and one media delivery loop per bot, always claiming by exact `(channel="lark", bot_id)`, with unique worker IDs, retry-safe outbound UUIDs, exact originating chat/thread destinations, lease renewal, retry/permanent/unknown outcomes, and account-local rate-limit/backoff handling. Lark replies must never consume WeChat reply slots or another bot's worker capacity.

### Phase 4: Compose all channel accounts in one supervisor
18. Generalize the production composition root in `src/codex_wechat_bot.py` into a multi-account supervisor while preserving `./cow` compatibility. Acquire one database ownership lock plus a sorted, atomic set of account locks for WeChat and every enabled `(channel="lark", bot_id)`; rollback all acquired locks if any account conflicts. Construct the store, principal resolver, `TaskManager`, global `AgentRegistry`, bridge, mailbox workers, and Agent processes exactly once.
19. Add a `ChannelAccountSupervisor` lifecycle per bot: `starting`, `ready`, `disconnected`, `restarting`, `failed`, `disabled`. Start shared runtime components first, then account-local delivery and ingress. A bot consumer crash, expired credential, rate limit, or malformed stream restarts/fails only that bot with bounded exponential backoff and generation fencing; peer bots, WeChat, durable tasks, and Agents continue.
20. Preserve fair service across bots sharing one serialized Agent. Add configurable unfinished-invocation limits per `(channel, bot_id)` and account-aware scheduling/round-robin admission so one noisy bot cannot monopolize global queue capacity, while retaining FIFO within each bot/Agent stream and the existing one-slot-per-Agent invariant.
21. Stop all account ingress first during full shutdown, drain/fence account delivery claims, stop shared dispatch/mailboxes and Agent processes, close the bridge/store epoch, then release account and database locks. An individual bot restart performs only account-local ingress and delivery lifecycle operations.
22. Add launcher/configuration compatibility aliases without immediately renaming existing `CODEX_WECHAT_*` variables or data paths. Document multi-bot QR commands and deprecate misleading names later rather than creating unrelated migration churn.

### Phase 5: Verification and rollout
23. Add QR onboarding tests with a fake pinned `lark-cli`: terminal QR/URL relay, successful scan, timeout, cancellation, malformed output, unsupported version, duplicate app ID, secret redaction, private permissions, atomic profile persistence, partial-write rollback, unattended restart, reauthorization, disable/enable, and removal with pending work.
24. Add multi-bot adapter tests: two or more isolated config directories and consumers, wrong-profile event rejection, stale-generation output rejection, independent cursors/dedupe, direct messages, structured group mentions, topic threads, attachments, exact bot/chat/thread reply targets, per-bot text/media claims, idempotent sends, rate-limit isolation, crash/restart, and clean shutdown.
25. Add routing/authority tests proving every bot lists the same enabled Agent catalog, each can switch to any existing Agent, Bot A's switch does not alter Bot B or WeChat, non-administrators cannot create/delete global Agents or select arbitrary config profiles, and administrator-created Agents become visible to all bots.
26. Add coexistence and fairness tests proving one shared store/registry/manager serves WeChat plus multiple Lark bots; identical external IDs do not collide across bot IDs; one failed or noisy bot cannot stop/starve peers; Lark never consumes WeChat quota; and principal mappings cannot redirect replies or synchronize account-local routes.
27. Run focused test groups after each phase, then the full pytest suite. Perform a live smoke test by adding two bots through separate terminal QR scans, verifying unattended restart, switching each bot to different Agents, exercising group threads and one file/image each direction, reauthorizing one bot while the other remains online, and confirming WeChat is unaffected.

**Relevant files**
- `/Users/autoclient/projects/codex-on-wechat/architecture.md` — document the multi-channel topology, identity layers, ownership, and corrected schema status.
- `/Users/autoclient/projects/codex-on-wechat/src/channels/models.py` — normalized ingress, reply target, delivery contracts, explicit channel/principal/conversation-subject fields.
- `/Users/autoclient/projects/codex-on-wechat/src/channels/wechat.py` — retain WeChat adapter behavior while extracting common commands and WeChat delivery policy.
- `/Users/autoclient/projects/codex-on-wechat/src/channels/lark.py` — new `lark-cli` process, gateway, normalization, media, and delivery implementation.
- `/Users/autoclient/projects/codex-on-wechat/src/channels/commands.py` — new shared command registry/router and channel capability filtering.
- `/Users/autoclient/projects/codex-on-wechat/src/runtime/identity.py` — canonical principal and conversation-subject compound identity helpers plus legacy candidates.
- `/Users/autoclient/projects/codex-on-wechat/src/runtime/manager.py` — resolve principals, use principal-scoped state/locks, preserve immutable transport reply targets.
- `/Users/autoclient/projects/codex-on-wechat/src/runtime/models.py` — principal snapshots and transport-neutral outbound records.
- `/Users/autoclient/projects/codex-on-wechat/src/runtime/store.py` — principal and channel-policy store contracts without iLink leakage.
- `/Users/autoclient/projects/codex-on-wechat/src/runtime/sqlite_store.py` — append migration, principal mappings/state keys, channel-specific reply projection, backward-compatible history.
- `/Users/autoclient/projects/codex-on-wechat/src/runtime/supervisor.py` — multiple account ownership and independent adapter lifecycle support.
- `/Users/autoclient/projects/codex-on-wechat/src/codex_wechat_bot.py` — shared multi-channel composition root and startup/shutdown ordering.
- `/Users/autoclient/projects/codex-on-wechat/tests/test_compound_identity.py` — extend delimiter/canonical principal coverage.
- `/Users/autoclient/projects/codex-on-wechat/tests/test_wechat_outbox.py` — prove WeChat projection remains unchanged and cross-channel IDs remain independent.
- `/Users/autoclient/projects/codex-on-wechat/tests/test_reply_quota.py` — prove quota is WeChat-specific.
- `/Users/autoclient/projects/codex-on-wechat/tests/test_principal_mapping.py` — new mapping, migration, spoofing, and conflict tests.
- `/Users/autoclient/projects/codex-on-wechat/tests/test_lark_normalization.py` — new event, mention, thread, and dedupe tests.
- `/Users/autoclient/projects/codex-on-wechat/tests/test_lark_cli_process.py` — new subprocess lifecycle and protocol tests.
- `/Users/autoclient/projects/codex-on-wechat/tests/test_lark_outbox.py` — new text/media delivery and exact-target tests.
- `/Users/autoclient/projects/codex-on-wechat/tests/test_multichannel_composition.py` — new end-to-end coexistence and failure-isolation tests.

**Verification**
1. Focused tests after command/delivery refactoring: existing WeChat command, outbox, reply quota, notification, media, and sender-wire invariant suites must remain green before Lark code is introduced.
2. Migration tests against a pre-feature database must preserve existing account-local routes, conversations, inbound IDs, outbox targets, tasks, and WeChat behavior exactly.
3. A fake pinned `lark-cli` must exercise terminal QR registration, credential/profile persistence, readiness, NDJSON events, send acknowledgements, malformed output, crash loops, generation fencing, backoff, reauthorization, disable/enable, and shutdown without live credentials.
4. Coexistence tests start WeChat and at least two fake Lark bot profiles over one SQLite store and global Agent registry, while proving account-local Agent routes, exact per-bot delivery, fair shared-Agent scheduling, and independent adapter recovery.
5. Run the complete repository test suite and inspect diagnostics for app-secret/token leakage, insecure profile permissions, stale claims, and orphaned subprocesses.
6. Live smoke test: add two bots through separate terminal QR scans, switch them to different shared Agents, restart unattended, reauthorize one bot without interrupting the other, exercise a group thread and attachments, and verify WeChat remains online.

**Decisions**
- WeChat and every Lark bot are peer user-interface accounts, not Agents. They share one global Agent catalog and process runtime.
- Multiple Lark/Feishu bots connect simultaneously. Each has an isolated CLI config directory, account lock, consumers, delivery workers, health state, restart policy, and exact `bot_id` filtering.
- The local OS owner adds or reauthorizes each bot through a terminal QR scan using only a version-pinned `lark-cli config init --new`; no web UI or Node helper is included.
- QR bot registration is PersonalAgent app provisioning/binding, not user OAuth. `lark-cli auth login` and user impersonation are excluded from the first milestone.
- Every bot can list and switch to any existing enabled Agent. Active Agent routes, conversations, modes, roles, models, and working directories remain per bot/account and chat; switching Bot A does not switch Bot B or WeChat.
- Dynamic Agent creation, profile selection, and deletion mutate the global registry and therefore require administrator authority.
- Canonical principal mappings support authorization/audit and future shared policy, but do not redirect replies, merge provider threads, or synchronize bot-local routes by default.
- Initial Lark scope includes text/common commands, direct/group interactions, topic/thread scoping, structured bot mentions, and basic attachments.
- Runtime transport uses version-pinned `lark-cli` subprocesses. Native Python OpenAPI/WebSocket, Node sidecars, cards/actions, and document comments are later phases.
- Ordinary replies always return through the exact originating bot and chat/thread. Existing `./cow`, WeChat behavior, environment variables, and data paths remain compatible.

**Further Considerations**
1. Pin a tested `lark-cli` release and vendor a machine-readable protocol fixture in tests; the implementation must not depend on unstable internal Go packages.
2. Use `open_id` scoped to the configured Lark app/tenant for account mapping, and store Lark/Feishu OpenAPI domain as profile configuration.
3. Plan a later phase for progress cards and signed action callbacks using the run-state reducer pattern from `lark-coding-agent-bridge`, after durable text/media delivery is stable.

import { assert, it, vi } from "@effect/vitest";
import {
  CheckpointId,
  CheckpointRef,
  CheckpointScopeId,
  EventId,
  MessageId,
  NodeId,
  ProjectId,
  ProviderInstanceId,
  RunId,
  ThreadId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";

import { SqlitePersistenceMemory } from "../persistence/Layers/Sqlite.ts";
import { checkpointRefForScopeOrdinal } from "../orchestration-v2/CheckpointService.ts";
import { OrchestratorProjectionError } from "../orchestration-v2/Orchestrator.ts";
import * as ProjectionStore from "../orchestration-v2/ProjectionStore.ts";
import * as ThreadManagement from "../orchestration-v2/ThreadManagementService.ts";
import * as CheckpointDiffQuery from "./CheckpointDiffQuery.ts";
import * as CheckpointStore from "./CheckpointStore.ts";

const cases = [
  { name: "baseline to first capture", from: 0, to: 1, reuse: true },
  { name: "adjacent captures", from: 1, to: 2, reuse: true },
  { name: "multiple turns", from: 0, to: 2, reuse: false },
  { name: "different scopes", from: 1, to: 2, differentScope: true, reuse: false },
  { name: "mismatched capture refs", from: 1, to: 2, mismatchedRef: true, reuse: false },
  { name: "summary without Git origins", from: 0, to: 1, noOrigins: true, reuse: false },
] as const;

for (const [name, layer] of [
  ["SQLite", ProjectionStore.layer.pipe(Layer.provide(SqlitePersistenceMemory))],
  ["memory", ProjectionStore.layerMemory],
] as const) {
  it.layer(layer)(`stored checkpoint attribution (${name})`, (it) => {
    for (const scenario of cases) {
      it.effect(`preserves filtered and full patches for ${scenario.name}`, () =>
        Effect.gen(function* () {
          const projections = yield* ProjectionStore.ProjectionStoreV2;
          const now = yield* DateTime.now;
          const threadId = ThreadId.make(`thread:${scenario.name}`);
          const scopeId = CheckpointScopeId.make(`scope:${scenario.name}`);
          const providerInstanceId = ProviderInstanceId.make("codex");
          const modelSelection = { instanceId: providerInstanceId, model: "gpt-5.4" };
          yield* projections.apply({
            id: EventId.make("thread-created"),
            type: "thread.created",
            threadId,
            occurredAt: now,
            payload: {
              createdBy: "user",
              creationSource: "web",
              id: threadId,
              projectId: ProjectId.make("project:attribution"),
              title: "Stored attribution",
              providerInstanceId,
              modelSelection,
              runtimeMode: "full-access",
              interactionMode: "default",
              branch: "feature",
              worktreePath: "/repo",
              activeProviderThreadId: null,
              lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: threadId },
              forkedFrom: null,
              createdAt: now,
              updatedAt: now,
              archivedAt: null,
              settledOverride: null,
              settledAt: null,
              lastVisitedAt: null,
              deletedAt: null,
            },
          });
          for (const ordinal of [1, 2]) {
            const runId = RunId.make(`run:${ordinal}`);
            const nodeId = NodeId.make(`node:${ordinal}`);
            const checkpointId = CheckpointId.make(`checkpoint:${ordinal}`);
            const differentScope = "differentScope" in scenario && ordinal === 2;
            const captureScopeId = differentScope ? CheckpointScopeId.make("scope:other") : scopeId;
            const ordinalWithinScope = differentScope ? 1 : ordinal;
            yield* projections.apply({
              id: EventId.make(`run:${ordinal}`),
              type: "run.created",
              threadId,
              occurredAt: now,
              payload: {
                id: runId,
                threadId,
                ordinal,
                providerInstanceId,
                modelSelection,
                providerThreadId: null,
                userMessageId: MessageId.make(`message:${ordinal}`),
                rootNodeId: nodeId,
                activeAttemptId: null,
                status: "completed",
                requestedAt: now,
                startedAt: now,
                completedAt: now,
                checkpointId,
                contextHandoffId: null,
              },
            });
            if (ordinal === 1 || differentScope) {
              yield* projections.apply({
                id: EventId.make(`scope:${ordinal}`),
                type: "checkpoint-scope.created",
                threadId,
                occurredAt: now,
                payload: {
                  id: captureScopeId,
                  threadId,
                  runId,
                  nodeId,
                  parentScopeId: null,
                  providerThreadId: null,
                  kind: "root_run",
                  ordinalWithinParent: ordinal - 1,
                  advancesAppRunCount: true,
                  cwd: "/repo",
                  createdAt: now,
                },
              });
            }
            yield* projections.apply({
              id: EventId.make(`checkpoint:${ordinal}`),
              type: "checkpoint.captured",
              threadId,
              occurredAt: now,
              payload: {
                id: checkpointId,
                threadId,
                scopeId: captureScopeId,
                runId,
                nodeId,
                parentCheckpointId: null,
                ordinalWithinScope,
                appRunOrdinal: ordinal,
                ref:
                  "mismatchedRef" in scenario && ordinal === 1
                    ? CheckpointRef.make("refs/t3/legacy/first")
                    : checkpointRefForScopeOrdinal({ scopeId: captureScopeId, ordinalWithinScope }),
                status: "ready",
                files: [
                  {
                    path: "upstream.ts",
                    kind: "modified",
                    additions: 1,
                    deletions: 0,
                    ...("noOrigins" in scenario ? {} : { origin: "git" as const }),
                  },
                ],
                capturedAt: now,
              },
            });
          }
          const getGitChangedPaths = vi.fn(() => Effect.succeed(["upstream.ts"]));
          const queryLayer = CheckpointDiffQuery.layer.pipe(
            Layer.provide(
              Layer.mock(ThreadManagement.ThreadManagementService)({
                getCheckpointContext: (threadId) =>
                  projections
                    .getCheckpointContext(threadId)
                    .pipe(
                      Effect.mapError(
                        (cause) => new OrchestratorProjectionError({ threadId, cause }),
                      ),
                    ),
                getThreadRecords: (threadId, fields, filter) =>
                  projections
                    .getThreadRecords(threadId, fields, filter)
                    .pipe(
                      Effect.mapError(
                        (cause) => new OrchestratorProjectionError({ threadId, cause }),
                      ),
                    ),
              }),
            ),
            Layer.provide(
              Layer.mock(CheckpointStore.CheckpointStore)({
                getGitChangedPaths,
                diffCheckpoints: (input) =>
                  Effect.succeed(
                    input.format === "numstat"
                      ? "1\t0\tupstream.ts\u00001\t1\tworkspace.ts\0"
                      : input.filePaths === undefined
                        ? "workspace patch\nupstream patch"
                        : input.filePaths
                            .map((path) =>
                              path === "upstream.ts" ? "upstream patch" : "workspace patch",
                            )
                            .join("\n"),
                  ),
              }),
            ),
          );
          const query = yield* CheckpointDiffQuery.CheckpointDiffQuery.pipe(
            Effect.provide(queryLayer),
          );
          const input = { threadId, fromTurnCount: scenario.from, toTurnCount: scenario.to };
          const filtered = yield* query.getTurnDiff({ ...input, includeGitChanges: false });
          assert.strictEqual(filtered.diff, "workspace patch");
          assert.strictEqual(filtered.gitFileCount, 1);
          const full = yield* query.getTurnDiff({ ...input, includeGitChanges: true });
          assert.strictEqual(full.diff, "workspace patch\nupstream patch");
          assert.strictEqual(full.gitFileCount, 1);
          assert.strictEqual(getGitChangedPaths.mock.calls.length, scenario.reuse ? 0 : 2);
          const legacy = yield* query.getTurnDiff(input);
          assert.strictEqual(legacy.diff, full.diff);
          assert.strictEqual(legacy.gitFileCount, undefined);
          assert.strictEqual(getGitChangedPaths.mock.calls.length, scenario.reuse ? 0 : 2);
        }),
      );
    }
  });
}

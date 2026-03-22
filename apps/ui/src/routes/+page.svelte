<script lang="ts">
  import QueryEditor from '$lib/QueryEditor.svelte';
  import Icon from '@iconify/svelte';
  import { invoke } from '@tauri-apps/api/core';

  type QueryResult =
    | { type: 'Rows'; data: { columns: string[]; rows: string[][] } }
    | { type: 'Count'; data: number }
    | { type: 'Success' }
    | { type: 'Error'; data: string };

  let query = $state('');
  let results = $state<QueryResult[]>([]);
  let running = $state(false);
  let elapsedMs = $state<number | null>(null);

  async function run() {
    if (!query.trim()) return;
    const startedAt = performance.now();
    running = true;
    try {
      results = await invoke<QueryResult[]>('run_query', { src: query });
    } catch (e) {
      results = [{ type: 'Error', data: String(e) }];
    } finally {
      elapsedMs = Math.round(performance.now() - startedAt);
      running = false;
    }
  }
</script>

<div class="flex h-screen flex-col">
  <div class="flex min-h-0 flex-1">
    <!-- Left: Query Editor -->
    <div class="border-base-300 flex w-1/2 flex-col border-r">
      <div class="border-base-300 flex items-center border-b px-4 py-2">
        <span class="text-md font-semibold">Query</span>
      </div>
      <div class="relative flex-1">
        <QueryEditor bind:value={query} onrun={run} />
        <div class="pointer-events-none absolute right-4 bottom-4 z-10">
          <button
            class="btn btn-success btn-circle btn-md pointer-events-auto shadow-lg"
            onclick={run}
            disabled={running || !query.trim()}
            aria-label="Run query"
          >
            {#if running}
              <span class="loading loading-spinner loading-xs"></span>
            {:else}
              <Icon icon="lucide:play" width={20} height={20} />
            {/if}
          </button>
        </div>
      </div>
    </div>

    <!-- Right: Result -->
    <div class="flex w-1/2 flex-col">
      <div class="border-base-300 border-b px-4 py-2">
        <span class="text-md font-semibold">Result</span>
      </div>
      <div class="flex-1 overflow-auto p-4 font-mono text-sm">
        {#if results.length === 0}
          <span class="text-base-content/40">Run a query to see results.</span>
        {:else}
          {#each results as result, i}
            <div class="mb-4">
              {#if result.type === 'Rows'}
                <div class="overflow-x-auto">
                  <table class="table table-xs table-zebra">
                    <thead>
                      <tr>
                        {#each result.data.columns as col}
                          <th>{col}</th>
                        {/each}
                      </tr>
                    </thead>
                    <tbody>
                      {#each result.data.rows as row}
                        <tr>
                          {#each row as cell}
                            <td>{cell}</td>
                          {/each}
                        </tr>
                      {/each}
                    </tbody>
                  </table>
                  <p class="text-base-content/50 mt-1 text-xs">{result.data.rows.length} row(s)</p>
                </div>
              {:else if result.type === 'Count'}
                <div class="badge badge-success badge-soft">{result.data} row(s) affected</div>
              {:else if result.type === 'Success'}
                <div class="badge badge-success badge-soft">OK</div>
              {:else if result.type === 'Error'}
                <div role="alert" class="alert alert-error alert-soft">
                  <span>{result.data}</span>
                </div>
              {/if}
            </div>
          {/each}
        {/if}
      </div>
    </div>
  </div>

  <div class="border-base-300 bg-base-200 text-base-content flex items-center justify-between border-t px-4 py-2 text-sm">
    <div class="flex items-center gap-2 font-medium">
      <kbd class="kbd kbd-sm">Cmd</kbd>
      <span>+</span>
      <kbd class="kbd kbd-sm">Enter</kbd>
      <span class="text-base-content/70">to run query</span>
    </div>
    <div class="font-mono text-xs text-base-content/70">
      {#if running}
        Running...
      {:else if elapsedMs !== null}
        {elapsedMs} ms
      {:else}
        -- ms
      {/if}
    </div>
  </div>
</div>

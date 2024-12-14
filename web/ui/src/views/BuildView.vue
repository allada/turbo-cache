<script setup lang="ts">
import { useBuildStore } from '@/stores/build'

const buildStore = useBuildStore()
</script>

<template>
  <div class="flex-1 h-screen w-full grow overflow-y-scroll p-10 pb-10 text-lg">
    <!-- Recent Builds -->
              <div class="rounded-lg border bg-card text-card-foreground shadow-sm h-full">
                <div class="flex flex-col space-y-1.5 p-6">
                  <h3 class="text-2xl leading-none text-white">Recent Builds</h3>
                </div>
                <div class="p-6 pt-0">
        <div
          id="build-list"
          class="h-72 overflow-y-scroll pr-[15px] flex gap-4 flex-col scrollbar scrollbar-track-card scrollbar-thumb-gray-600"
        >
          <!-- Iterate over builds -->
          <router-link
            v-for="build in buildStore.builds"
            :key="build.id"
            :to="`/builds/${build.id}`"
            class="block p-4 mb-4 rounded-lg border transition-colors text-white"
          >
            <h4 class="text-xl font-semibold">{{ build.name }}</h4>
            <p
              :class="{
                'text-[green]': build.status === 'Success',
                'text-[yellow]': build.status === 'In Progress',
                'text-[red]': build.status === 'Failed'
              }"
              class="text-sm"
            >
              {{ build.status }}
            </p>          </router-link>

          <!-- If there are no builds, show a message -->
          <p v-if="buildStore.builds.length === 0" class="text-center text-gray-500">No build events</p>
        </div>
      </div>
              </div>
            </div>
</template>

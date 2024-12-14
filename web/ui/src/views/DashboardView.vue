<script setup lang="ts">
import BuildStatistics from '../components/dashboard/BuildStatistics.vue';
import RecentBuilds from '../components/dashboard/RecentBuilds.vue';
import RemoteCacheUsage from '../components/dashboard/RemoteCacheUsage.vue';
</script>

<template>
     <main class="flex-1 h-screen w-full grow overflow-y-scroll p-10 pb-10 text-lg">
        <div class="pb-10">
          <h2 class="scroll-m-20 pb-10 text-3xl text-white first:mt-0">Dashboard</h2>
          <div class="grid grid-cols-2 gap-10 pb-7">

            <BuildStatistics />
            <RecentBuilds />
            <RemoteCacheUsage />

          </div>
        </div>
      </main>
</template>

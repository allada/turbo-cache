<template>
    <div class="p-10">
      <h1 class="text-3xl font-bold mb-4 text-white">
        Build Details for: {{ build?.name || 'Unknown' }}
      </h1>
      <div class="mt-6 text-white">
        <p>
        <strong>Status: </strong>
        <span :class="statusClass(build?.status)" class="text-sm font-semibold">
           {{ build?.status || 'N/A' }}
        </span>
      </p>
        <p>
          <strong>Start Time:</strong> {{ build?.startTime }}
        </p>
        <p>
          <strong>End Time:</strong> {{ build?.endTime }}
        </p>
      </div>
      <router-link to="/builds" class="mt-4 inline-block text-blue-500 hover:underline">
        &larr; Back to Builds
      </router-link>
    </div>
  </template>

  <script setup lang="ts">
  import { useRoute } from 'vue-router'
  import { useBuildStore } from '@/stores/build'
  import { computed } from 'vue'

  // Access the current route to extract the buildId
  const route = useRoute()
  const buildStore = useBuildStore()

  // Extract buildId from route parameters
  const buildId = computed(() => route.params.buildId as string)

  // Retrieve the specific build from the store
  const build = computed(() => buildStore.builds.find(b => b.id === buildId.value))

  const statusClass = (status: string | undefined): string => {
  switch (status) {
    case 'Success':
      return 'text-green-600'
    case 'In Progress':
      return 'text-yellow-600'
    case 'Failed':
      return 'text-red-600'
    default:
      return 'text-gray-600'
  }
}
</script>

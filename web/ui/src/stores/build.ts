import { defineStore } from 'pinia'
import { ref } from 'vue'

// Define the structure of a Build
export interface Build {
  id: string
  name: string
  status: 'Success' | 'Failed' | 'In Progress'
  startTime: string
  endTime: string
}

// Define the Pinia store
export const useBuildStore = defineStore('build', () => {
  // Reactive array to hold build data
  const builds = ref<Build[]>([
    {
      id: 'build-001',
      name: 'Build 001',
      status: 'Success',
      startTime: '2024-10-25 10:00 AM',
      endTime: '2024-10-25 10:30 AM'
    },
    {
      id: 'build-002',
      name: 'Build 002',
      status: 'Failed',
      startTime: '2024-10-26 11:00 AM',
      endTime: '2024-10-26 11:45 AM'
    },
    // Add more dummy builds as needed
  ])

  // Optional: Actions to manipulate builds
  const addBuild = (build: Build) => {
    builds.value.push(build)
  }

  const removeBuild = (id: string) => {
    builds.value = builds.value.filter(build => build.id !== id)
  }

  return { builds, addBuild, removeBuild }
})

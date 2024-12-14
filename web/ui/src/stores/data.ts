import { ref, computed } from 'vue'
import { defineStore } from 'pinia'

export const useDataStore = defineStore('data', () => {
  const rawData = ref(0)
  //TODO: Add raw data store, which retrieves data from httpStore and webSocketStore and push them in to buildStore

  return { rawData }
})

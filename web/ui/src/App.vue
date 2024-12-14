<script setup lang="ts">
import AsideView from './views/AsideView.vue';
</script>

<template>
  <div class="min-h-screen w-full bg-background bg-radial-gradient font-sans antialiased">
    <div class="flex overflow-y-auto">
      <!-- Sidebar -->
      <AsideView />
      <!-- Main Content -->
      <transition name="fade" mode="out-in">
        <RouterView />
      </transition>
    </div>
  </div>
</template>

<style scoped>
.fade-enter-active, .fade-leave-active {
  transition: opacity 0.5s;
}
.fade-enter-from, .fade-leave-to {
  opacity: 0;
}
.fade-enter-to, .fade-leave-from {
  opacity: 1;
}
</style>

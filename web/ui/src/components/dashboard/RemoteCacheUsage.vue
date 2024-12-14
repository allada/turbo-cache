<script setup lang="ts">
</script>

<template>
    <div class="col-span">
        <!-- Remote Cache Usage -->
              <div class="rounded-lg border bg-card text-card-foreground shadow-sm h-full">
                <div class="flex flex-col space-y-1.5 p-6">
                  <h3 class="text-2xl leading-none text-white">Remote Cache Usage</h3>
                  <p class="text-sm text-muted-foreground text-white">
                    Updates every 60 seconds
                  </p>
                </div>
                <div class="p-6 pt-0">
                  <div class="h-72">
                    <div
                      class="py-8 text-center text-2xl text-white"
                      data-testid="ingress"
                    >
                      Ingress: 0 Bytes / ∞
                    </div>
                    <div
                      class="py-8 text-center text-2xl text-white"
                      data-testid="egress"
                    >
                      Egress: 0 Bytes / 1 TB
                    </div>
                  </div>
                </div>
              </div>
            </div>
</template>

<style scoped>
/* Custom scrollbar styles */
.scrollbar::-webkit-scrollbar {
  width: 8px;
}
.scrollbar::-webkit-scrollbar-track {
  background: var(--tw-color-card);
}
.scrollbar::-webkit-scrollbar-thumb {
  background-color: #4b5563; /* Gray-600 */
  border-radius: 4px;
}

/* Hide scrollbar for no-scrollbar class */
.no-scrollbar::-webkit-scrollbar {
  display: none;
}
.no-scrollbar {
  -ms-overflow-style: none; /* IE and Edge */
  scrollbar-width: none; /* Firefox */
}

/* Utility for radial gradient background */
.bg-radial-gradient {
  background-image: radial-gradient(
    farthest-corner at 42.5% -20%,
    rgba(40, 41, 79, 255) 5%,
    rgba(11, 12, 17, 255) 65%
  );
}

/* Visually hidden class for accessibility */
.sr-only {
  border: 0;
  clip: rect(0 0 0 0);
  height: 1px;
  margin: -1px;
  overflow: hidden;
  padding: 0;
  position: absolute;
  width: 1px;
  white-space: nowrap;
}

/* Link styles */
.link {
  text-decoration: none;
  color: inherit;
}
</style>

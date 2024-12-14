<script setup lang="ts">
import logoLightSrc from '../assets/logo-light.svg';
import logoDarkSrc from '../assets/logo-dark.svg';

const logoLight = logoLightSrc;
const logoDark = logoDarkSrc;
</script>

<template>
<aside class="no-scrollbar flex h-screen w-72 flex-col justify-between overflow-y-auto border-solid border-r-1 pb-10">
    <div class="space-y-4 py-5">
      <!-- Logo -->
      <div class="pl-10 pr-6 pt-7">
            <div class="mb-8 px-2">
                <a href="https://nativelink.com" target="_blank">
                <img
                    alt="Logo light"
                    class="block dark:hidden"
                    :src="logoLight"
                    width="180"
                    height="64"
                    decoding="async"
                    style="color: transparent;"
                />
                <img
                    alt="Logo dark"
                    class="hidden dark:block"
                    :src="logoDark"
                    width="180"
                    height="64"
                    decoding="async"
                    style="color: transparent;"
                />
                </a>
            </div>
            <!-- Navigation Links -->
            <nav class="ml-2 space-y-1 w-full">
                <RouterLink to="/"
                class="inline-flex items-center whitespace-nowrap rounded-md font-medium ring-offset-background transition-colors focus:outline-none focus:ring-2 focus:ring-ring focus:ring-offset-2 hover:bg-accent hover:text-accent-foreground px-4 py-2 w-full justify-start text-base h-14 cursor-pointer bg-accent text-accent-foreground"
                >
                <!-- Dashboard Icon -->
                <svg
                xmlns="http://www.w3.org/2000/svg"
                width="24"
                height="24"
                viewBox="0 0 24 24"
                fill="none"
                stroke="currentColor"
                stroke-width="2"
                stroke-linecap="round"
                stroke-linejoin="round"
                class="lucide lucide-layout-dashboard nav-icon mr-4 h-7 w-7"
                >
                <rect width="7" height="9" x="3" y="3" rx="1"></rect>
                <rect width="7" height="5" x="14" y="3" rx="1"></rect>
                <rect width="7" height="9" x="14" y="12" rx="1"></rect>
                <rect width="7" height="5" x="3" y="16" rx="1"></rect>
                </svg>
                Dashboard
            </RouterLink>
            <RouterLink to="/builds"
                class="inline-flex items-center whitespace-nowrap rounded-md font-medium ring-offset-background transition-colors focus:outline-none focus:ring-2 focus:ring-ring focus:ring-offset-2 hover:bg-accent hover:text-accent-foreground px-4 py-2 w-full justify-start text-base h-14 cursor-pointer bg-accent text-accent-foreground"
                >
                <!-- Dashboard Icon -->
                <svg
                xmlns="http://www.w3.org/2000/svg"
                width="24"
                height="24"
                viewBox="0 0 24 24"
                fill="none"
                stroke="currentColor"
                stroke-width="2"
                stroke-linecap="round"
                stroke-linejoin="round"
                class="lucide lucide-layout-dashboard nav-icon mr-4 h-7 w-7"
                >
                <rect width="7" height="9" x="3" y="3" rx="1"></rect>
                <rect width="7" height="5" x="14" y="3" rx="1"></rect>
                <rect width="7" height="9" x="14" y="12" rx="1"></rect>
                <rect width="7" height="5" x="3" y="16" rx="1"></rect>
                </svg>
                Builds
            </RouterLink>
            <hr />
            <a
                class="inline-flex items-center whitespace-nowrap rounded-md font-medium ring-offset-background transition-colors focus:outline-none focus:ring-2 focus:ring-ring focus:ring-offset-2 hover:bg-accent hover:text-accent-foreground px-4 py-2 w-full justify-start text-muted-foreground text-base h-14 cursor-pointer"
                target="_blank"
                href="https://www.nativelink.com/docs/introduction/setup"
            >
                <!-- Documentation Icon -->
                <svg
                xmlns="http://www.w3.org/2000/svg"
                width="24"
                height="24"
                viewBox="0 0 24 24"
                fill="none"
                stroke="currentColor"
                stroke-width="2"
                stroke-linecap="round"
                stroke-linejoin="round"
                class="lucide lucide-book-text nav-icon mr-4 h-7 w-7"
                >
                <path d="M4 19.5v-15A2.5 2.5 0 0 1 6.5 2H20v20H6.5a2.5 2.5 0 0 1 0-5H20"></path>
                <path d="M8 7h6"></path>
                <path d="M8 11h8"></path>
                </svg>
                Documentation
            </a>
            </nav>
      </div>
    </div>
  </aside>
</template>

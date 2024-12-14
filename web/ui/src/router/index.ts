import { createRouter, createWebHistory } from 'vue-router'
import DashboardView from '@/views/DashboardView.vue'
import BuildDetailView from '@/views/BuildDetailView.vue'


const router = createRouter({
  history: createWebHistory(import.meta.env.BASE_URL),
  routes: [
    {
      path: '/',
      name: 'dashboard',
      component: DashboardView
    },
    {
      path: '/builds',
      name: 'builds',
      // route level code-splitting
      // this generates a separate chunk (About.[hash].js) for this route
      // which is lazy-loaded when the route is visited.
      component: () => import('@/views/BuildView.vue')
    },
    {
      path: '/builds/:buildId',
      name: 'buildDetail',
      component: BuildDetailView,
    },
  ]
})

export default router

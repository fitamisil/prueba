import Vue from 'vue'
import Router from 'vue-router'
import { normalizeURL, decode } from 'ufo'
import { interopDefault } from './utils'
import scrollBehavior from './router.scrollBehavior.js'

const _10e9c804 = () => interopDefault(import('../pages/alarms.vue' /* webpackChunkName: "pages/alarms" */))
const _660c72f0 = () => interopDefault(import('../pages/dashboard.vue' /* webpackChunkName: "pages/dashboard" */))
const _68006351 = () => interopDefault(import('../pages/devices.vue' /* webpackChunkName: "pages/devices" */))
const _14573246 = () => interopDefault(import('../pages/login.vue' /* webpackChunkName: "pages/login" */))
const _19b7b236 = () => interopDefault(import('../pages/provisioner.vue' /* webpackChunkName: "pages/provisioner" */))
const _36bf68df = () => interopDefault(import('../pages/register.vue' /* webpackChunkName: "pages/register" */))
const _261276a6 = () => interopDefault(import('../pages/templates.vue' /* webpackChunkName: "pages/templates" */))
const _9365bd64 = () => interopDefault(import('../pages/test.vue' /* webpackChunkName: "pages/test" */))
const _46b189ca = () => interopDefault(import('../pages/GeneralViews/NotFoundPage.vue' /* webpackChunkName: "pages/GeneralViews/NotFoundPage" */))
const _2c2cc3c6 = () => interopDefault(import('../pages/index.vue' /* webpackChunkName: "pages/index" */))

const emptyFn = () => {}

Vue.use(Router)

export const routerOptions = {
  mode: 'history',
  base: '/',
  linkActiveClass: 'nuxt-link-active',
  linkExactActiveClass: 'active',
  scrollBehavior,

  routes: [{
    path: "/alarms",
    component: _10e9c804,
    name: "alarms"
  }, {
    path: "/dashboard",
    component: _660c72f0,
    name: "dashboard"
  }, {
    path: "/devices",
    component: _68006351,
    name: "devices"
  }, {
    path: "/login",
    component: _14573246,
    name: "login"
  }, {
    path: "/provisioner",
    component: _19b7b236,
    name: "provisioner"
  }, {
    path: "/register",
    component: _36bf68df,
    name: "register"
  }, {
    path: "/templates",
    component: _261276a6,
    name: "templates"
  }, {
    path: "/test",
    component: _9365bd64,
    name: "test"
  }, {
    path: "/GeneralViews/NotFoundPage",
    component: _46b189ca,
    name: "GeneralViews-NotFoundPage"
  }, {
    path: "/",
    component: _2c2cc3c6,
    name: "index"
  }],

  fallback: false
}

export function createRouter (ssrContext, config) {
  const base = (config._app && config._app.basePath) || routerOptions.base
  const router = new Router({ ...routerOptions, base  })

  // TODO: remove in Nuxt 3
  const originalPush = router.push
  router.push = function push (location, onComplete = emptyFn, onAbort) {
    return originalPush.call(this, location, onComplete, onAbort)
  }

  const resolve = router.resolve.bind(router)
  router.resolve = (to, current, append) => {
    if (typeof to === 'string') {
      to = normalizeURL(to)
    }
    return resolve(to, current, append)
  }

  return router
}

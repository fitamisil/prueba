export { default as Badge } from '../../components/Badge.vue'
export { default as BaseAlert } from '../../components/BaseAlert.vue'
export { default as BaseButton } from '../../components/BaseButton.vue'
export { default as BaseDropdown } from '../../components/BaseDropdown.vue'
export { default as BasePagination } from '../../components/BasePagination.vue'
export { default as BaseProgress } from '../../components/BaseProgress.vue'
export { default as BaseSwitch } from '../../components/BaseSwitch.vue'
export { default as BaseTable } from '../../components/BaseTable.vue'
export { default as CloseButton } from '../../components/CloseButton.vue'
export { default as Json } from '../../components/Json.vue'
export { default as LoadingPanel } from '../../components/LoadingPanel.vue'
export { default as Modal } from '../../components/Modal.vue'
export { default as Breadcrumb } from '../../components/Breadcrumb/Breadcrumb.vue'
export { default as BreadcrumbItem } from '../../components/Breadcrumb/BreadcrumbItem.vue'
export { default as BreadcrumbRouteBreadcrumb } from '../../components/Breadcrumb/RouteBreadcrumb.vue'
export { default as CardsCard } from '../../components/Cards/Card.vue'
export { default as DashboardTaskList } from '../../components/Dashboard/TaskList.vue'
export { default as DashboardUserTable } from '../../components/Dashboard/UserTable.vue'
export { default as InputsBaseCheckbox } from '../../components/Inputs/BaseCheckbox.vue'
export { default as InputsBaseInput } from '../../components/Inputs/BaseInput.vue'
export { default as InputsBaseRadio } from '../../components/Inputs/BaseRadio.vue'
export { default as InputsIconCheckbox } from '../../components/Inputs/IconCheckbox.vue'
export { default as LayoutContent } from '../../components/Layout/Content.vue'
export { default as LayoutContentFooter } from '../../components/Layout/ContentFooter.vue'
export { default as LayoutDashboardLayout } from '../../components/Layout/DashboardLayout.vue'
export { default as LayoutDashboardNavbar } from '../../components/Layout/DashboardNavbar.vue'
export { default as LayoutLoadingMainPanel } from '../../components/Layout/LoadingMainPanel.vue'
export { default as LayoutSidebarSharePlugin } from '../../components/Layout/SidebarSharePlugin.vue'
export { default as NavbarBaseNav } from '../../components/Navbar/BaseNav.vue'
export { default as NavbarToggleButton } from '../../components/Navbar/NavbarToggleButton.vue'
export { default as Notification } from '../../components/NotificationPlugin/Notification.vue'
export { default as NotificationPluginNotifications } from '../../components/NotificationPlugin/Notifications.vue'
export { default as NotificationPlugin } from '../../components/NotificationPlugin/index.js'
export { default as SidebarPluginSideBar } from '../../components/SidebarPlugin/SideBar.vue'
export { default as SidebarItem } from '../../components/SidebarPlugin/SidebarItem.vue'
export { default as UserProfileEditProfileForm } from '../../components/UserProfile/EditProfileForm.vue'
export { default as UserCard } from '../../components/UserProfile/UserCard.vue'
export { default as WidgetsIotbutton } from '../../components/Widgets/Iotbutton.vue'
export { default as WidgetsIotindicator } from '../../components/Widgets/Iotindicator.vue'
export { default as WidgetsIotswitch } from '../../components/Widgets/Iotswitch.vue'
export { default as WidgetsRtnumberchart } from '../../components/Widgets/Rtnumberchart.vue'
export { default as LayoutStarterSampleFooter } from '../../components/Layout/starter/SampleFooter.vue'
export { default as LayoutStarterSampleNavbar } from '../../components/Layout/starter/SampleNavbar.vue'

// nuxt/nuxt.js#8607
function wrapFunctional(options) {
  if (!options || !options.functional) {
    return options
  }

  const propKeys = Array.isArray(options.props) ? options.props : Object.keys(options.props || {})

  return {
    render(h) {
      const attrs = {}
      const props = {}

      for (const key in this.$attrs) {
        if (propKeys.includes(key)) {
          props[key] = this.$attrs[key]
        } else {
          attrs[key] = this.$attrs[key]
        }
      }

      return h(options, {
        on: this.$listeners,
        attrs,
        props,
        scopedSlots: this.$scopedSlots,
      }, this.$slots.default)
    }
  }
}

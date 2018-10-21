import { Switch, Route, HashRouter as Router } from 'react-router-dom';
import React from 'react';

import routerConfig from './routerConfig';

function recursiveRouterConfigV4(config = []) {
  const routeMap = [];
  config.forEach((item) => {
    const route = {
      path: item.path,
      component: item.component,
    };
    if (Array.isArray(item.children)) {
      route.childRoutes = recursiveRouterConfigV4(item.children);
    }
    routeMap.push(route);
  });
  return routeMap;
}

function renderRouterConfigV4(container, router, contextPath) {
  const routeChildren = [];
  const renderRoute = (routeContainer, routeItem, routeContextPath) => {
    let routePath;
    if (!routeItem.path) {
      // eslint-disable-next-line
      console.error('route must has `path`');
    } else if (routeItem.path === '/' || routeItem.path === '*') {
      routePath = routeItem.path;
    } else {
      routePath = `/${routeContextPath}/${routeItem.path}`.replace(/\/+/g, '/');
    }

    if (routeItem.layout && routeItem.component) {
      routeChildren.push(
        <Route
          key={routePath}
          exact
          path={routePath}
          render={(props) => {
            return React.createElement(
              routeItem.layout,
              props,
              React.createElement(routeItem.component, props)
            );
          }}
        />
      );
    } else if (routeContainer && routeItem.component) {
      routeChildren.push(
        <Route
          key={routePath}
          exact
          path={routePath}
          render={(props) => {
            return React.createElement(
              routeContainer,
              props,
              React.createElement(routeItem.component, props)
            );
          }}
        />
      );
    } else {
      routeChildren.push(
        <Route
          key={routePath}
          exact
          path={routePath}
          component={routeItem.component}
        />
      );
    }

    if (Array.isArray(routeItem.childRoutes)) {
      routeItem.childRoutes.forEach((r) => {
        renderRoute(routeItem.component, r, routePath);
      });
    }
  };

  router.forEach((r) => {
    renderRoute(container, r, contextPath);
  });

  return <Switch>{routeChildren}</Switch>;
}

const routerWithReactRouter4 = recursiveRouterConfigV4(routerConfig);
const routeChildren = renderRouterConfigV4(null, routerWithReactRouter4, '/');
export default <Router>{routeChildren}</Router>;

import Home from './pages/Home';
import Predict from './pages/Predict';
import Metric from './pages/Metric';
import NotFound from './pages/NotFound';


const routerConfig = [
  {
    path: '/',
    component: Home,
  },
  {
    path: '/predict/:data',
    component: Predict,
  },
  {
    path: '/metric',
    component: Metric,
  },
  {
    path: '*',
    component: NotFound,
  },
];

export default routerConfig;

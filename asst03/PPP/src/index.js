import ReactDOM from 'react-dom';
import router from './router';

const ICE_CONTAINER = document.getElementById('ice-container');

if (!ICE_CONTAINER) {
  throw new Error('No <div id="ice-container"></div> is founf in current page.');
}

ReactDOM.render(router, ICE_CONTAINER);

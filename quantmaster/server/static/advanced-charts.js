import {loadScript, loadStyle} from './workspace-loader.js';

let resources;

export function loadAdvancedCharts() {
  if (!resources) {
    const pending = Promise.all([
      loadStyle('/static/charts.css'),
      loadScript('/static/echarts.min.js').then(() => loadScript('/static/charts.js')),
    ]).catch(error => {
      if (resources === pending) resources = null;
      throw error;
    });
    resources = pending;
  }
  return resources;
}

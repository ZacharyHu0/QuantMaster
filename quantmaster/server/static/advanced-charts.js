import {loadScript, loadStyle} from './workspace-loader.js';

let resources;

export function loadAdvancedCharts() {
  if (!resources) {
    resources = Promise.all([
      loadStyle('/static/charts.css'),
      loadScript('/static/echarts.min.js').then(() => loadScript('/static/charts.js')),
    ]);
  }
  return resources;
}

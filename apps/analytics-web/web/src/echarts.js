import * as echarts from "echarts/core";
import { BarChart, LineChart, RadarChart } from "echarts/charts";
import {
  GridComponent,
  LegendComponent,
  TitleComponent,
  ToolboxComponent,
  TooltipComponent
} from "echarts/components";
import { CanvasRenderer } from "echarts/renderers";

echarts.use([
  BarChart,
  LineChart,
  RadarChart,
  GridComponent,
  LegendComponent,
  TitleComponent,
  ToolboxComponent,
  TooltipComponent,
  CanvasRenderer
]);

export default echarts;

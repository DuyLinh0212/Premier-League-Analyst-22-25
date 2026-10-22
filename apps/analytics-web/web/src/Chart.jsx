import ReactEChartsCore from "echarts-for-react/esm/core";
import echarts from "./echarts.js";

export default function Chart(props) {
  return <ReactEChartsCore echarts={echarts} {...props} onChartReady={(chart) => {
    chart.on("finished", () => { chart.getDom().dataset.chartReady = "true"; });
    props.onChartReady?.(chart);
  }} />;
}

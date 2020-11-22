import '../css/main.css';
import * as d3 from "d3";
import { LineChart, AreaChart, Area, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend } from 'recharts';
import sector_data from '../datasets/s&p500_sectors.json';
import index_data from '../datasets/s&p500_index.json';

function Container() {

  return (
    <div className="main-container">
      <br></br>
      <AreaChart
        width={1000}
        height={600}
        data={sector_data}
        margin={{
          top: 10, right: 30, left: 0, bottom: 0,
        }}
      >
        <CartesianGrid strokeDasharray="3 3" />
        <XAxis dataKey="name" />
        <YAxis />
        <Tooltip />
        <Area type="monotone" dataKey="XLB Materials" stackId="1" stroke="red" fill="red" />
        <Area type="monotone" dataKey="XLC Communication Services" stackId="1" stroke="green" fill="green" />
        <Area type="monotone" dataKey="XLE Energy" stackId="1" stroke="orange" fill="orange" />
      </AreaChart>
    </div>
  );
}

export default Container;
import '../css/main.css';
import { LineChart, Line, XAxis, YAxis, Tooltip, Legend } from 'recharts';
import {React, Component} from 'react';
import sector_data from '../datasets/s&p500_sectors.json';

export default class StockCovid extends Component { 
    render() {
    return (
      <div className="viz-container">
        <br></br>
        <LineChart
          width={500}
          height={300}
          data={sector_data}
          margin={{
            top: 5, right: 30, left: 20, bottom: 5,
          }}
        >
        <XAxis dataKey="name" />
        <YAxis yAxisId="left" />
        <YAxis yAxisId="right" orientation="right" />
        <Tooltip />
        <Legend />
        <Line yAxisId="left" type="monotone" dataKey="pv" stroke="#8884d8" dot={false}/>
        <Line yAxisId="right" type="monotone" dataKey="uv" stroke="#82ca9d" dot={false}/>
      </LineChart>
      </div>
    );
  }
  }
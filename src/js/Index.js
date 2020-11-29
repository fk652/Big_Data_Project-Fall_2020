import '../css/main.css';
import { AreaChart, Area, XAxis, YAxis, Tooltip, Legend } from 'recharts';
import {React, Component} from 'react';
import index_data from '../datasets/s&p500_index.json';

export default class Index extends Component { 
  render() {
  return (
    <div className="main-container">
      <br></br>
      <AreaChart width={1000} height={600} data={index_data}
        margin={{ top: 10, right: 30, left: 0, bottom: 0 }}>
        <defs>
          <linearGradient id="colorUv" x1="0" y1="0" x2="0" y2="1">
            <stop offset="5%" stopColor="#8884d8" stopOpacity={0.8}/>
            <stop offset="95%" stopColor="#8884d8" stopOpacity={0}/>
          </linearGradient>
          <linearGradient id="colorPv" x1="0" y1="0" x2="0" y2="1">
            <stop offset="5%" stopColor="#82ca9d" stopOpacity={0.8}/>
            <stop offset="95%" stopColor="#82ca9d" stopOpacity={0}/>
          </linearGradient>
        </defs>
        <XAxis dataKey="Date" />
        <YAxis />
        <Tooltip />
        <Area type="monotone" dataKey="S&P 500 Index" stroke="#82ca9d" fillOpacity={1} fill="url(#colorPv)" />
      </AreaChart>
    </div>
  );
}
}
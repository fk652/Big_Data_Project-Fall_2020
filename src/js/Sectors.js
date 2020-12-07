import '../css/main.css';
import { LineChart, Line, XAxis, YAxis, Tooltip, Legend } from 'recharts';
import {React, Component } from 'react';
import data from '../datasets/s&p500_sectors.json';

export default class Sectors extends Component { 

  render() {
    return (
    <div className="viz-container">
      <br></br>
      <LineChart
        width={750}
        height={500}
        data={data}
        margin={{
          top: 5, right: 30, left: 20, bottom: 5,
        }}
      >
        <XAxis dataKey="Date" />
        <YAxis />
        <Tooltip />
        <Legend />
        <Line type="monotone" dataKey="XLB Materials" stroke="#8884d8" dot={false}/>
        <Line type="monotone" dataKey="XLC Communication Services" stroke="#82ca9d" dot={false}/>
        <Line type="monotone" dataKey="XLE Energy" stroke="#ff6816" dot={false}/>
        <Line type="monotone" dataKey="XLF Financials" stroke="#b53232" dot={false}/>
        <Line type="monotone" dataKey="XLI Industrials" stroke="#9f7dd4" dot={false}/>
        <Line type="monotone" dataKey="XLK Technology" stroke="#478583" dot={false}/>
        <Line type="monotone" dataKey="XLP Consumer Staples" stroke="#121054" dot={false}/>
        <Line type="monotone" dataKey="XLRE Real Estate" stroke="#92108c" dot={false}/>
        <Line type="monotone" dataKey="XLU Utilities" stroke="#00074a" dot={false}/>
        <Line type="monotone" dataKey="XLV Health Care" stroke="#3664ff" dot={false}/>
        <Line type="monotone" dataKey="XLY Consumer Discretionary" stroke="#00298c" dot={false}/>
      </LineChart>
    </div>
  );
  }
}
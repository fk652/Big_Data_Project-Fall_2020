import {React, Component } from 'react';
import {BarChart, Bar, Cell, XAxis, YAxis, Tooltip, Legend, ReferenceLine } from 'recharts';
import '../css/main.css';
import aggregate_data from '../datasets/S&P_500_aggregate_stock_data.json';

export default class Example extends Component {
  
    render() {
      return (
        <div className="viz-container">
        <BarChart
          width={1000}
          height={600}
          data={aggregate_data}
          margin={{
            top: 5, right: 30, left: 20, bottom: 5,
          }}
        >
          <XAxis dataKey="Date" />
          <YAxis />
          <Tooltip />
          <Legend />
          <ReferenceLine y={0} stroke="#000" />
          <Bar dataKey="Daily Return" fill="#E1B505" />
        </BarChart>
        </div>
      );
    }
  }
  
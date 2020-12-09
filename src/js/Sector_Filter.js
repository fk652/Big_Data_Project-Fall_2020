import '../css/main.css';
import { AreaChart, Area, XAxis, YAxis, Tooltip, Legend } from 'recharts';
import sector_data from '../datasets/s&p500_sectors.json';
import {React, Component} from 'react';

export default class Sector_Filter extends Component { 

    constructor(props) {
        super(props);
        this.state = {value: 'XLB Materials'};
    
        this.handleChange = this.handleChange.bind(this);
      }
    
      handleChange(event) {
        this.setState({value: event.target.value});
      }


    render() {
      return (
        <div className="viz-container">
        <select value={this.state.value} onChange={this.handleChange}>
            <option value="XLB Materials">XLB Materials</option>
            <option value="XLC Communication Services">XLC Communication Services</option>
            <option value="XLE Energy">XLE Energy</option>
            <option value="XLF Financials">XLF Financials</option>
            <option value="XLI Industrials">XLI Industrials</option>
            <option value="XLK Technology">XLK Technology</option>
            <option value="XLP Consumer Staples">XLP Consumer Staples</option>
            <option value="XLRE Real Estate">XLRE Real Estate</option>
            <option value="XLU Utilities">XLU Utilities</option>
            <option value="XLV Health Care">XLV Health Care</option>
            <option value="XLY Consumer Discretionary">XLY Consumer Discretionary</option>
          </select>
        <br></br>
        <AreaChart width={700} height={500} data={sector_data}
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
        <Area type="monotone" dataKey={this.state.value} stroke="#82ca9d" fillOpacity={1} fill="url(#colorPv)" />
      </AreaChart>
    </div>
  );
  }
}
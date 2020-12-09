import '../css/main.css';
import { ScatterChart, Scatter, XAxis, YAxis, CartesianGrid, Tooltip, LabelList, Legend } from 'recharts';
import {React, Component} from 'react';
import data from '../datasets/consumer_discretionary_groups.json';

let group0 = [];
let group1 = [];
let group2 = [];
let group3 = [];
let group4 = [];

const clusterData = (data) => {
  let i = 0;
  for (i=0; i<data.length; i++){
    if (data[i]["Group"] === 0){
      group0.push(data[i])
    }
    if (data[i]["Group"] === 1){
      group1.push(data[i])
    }
    if (data[i]["Group"] === 2){
      group2.push(data[i])
    }
    if (data[i]["Group"] === 3){
      group3.push(data[i])
    }
    if (data[i]["Group"] === 4){
      group4.push(data[i])
    }
  }
};

const getCompanyName = (r,v) => {
  let i = 0;
  for (i=0; i<data.length; i++){
    if (data[i]["returns"] === r && data[i]["variances"] === v){
      let array = [];
      array.push(data[i]["Name"])
      array.push(data[i]["GICS Sub-Industry"])
      return array;
    }
  }
};

  
const CustomToolTip = ({ active, payload, label }) => {
  if (active) {
    let r = payload[0].value;
    let v = payload[1].value;
    return (
      <div className="custom-tooltip-scatter">
        <p className="label">Company Name: {getCompanyName(r,v)[0]}</p>
        <p className="label">Sub-Industry: {getCompanyName(r,v)[1]}</p>
        <p className="label">Returns: {`${payload[0].value}`}</p>
        <p className="label">Variances: {`${payload[1].value}`}</p>
      </div>
    );
  }
  return null;
};

export default class ConsumerDiscretionary extends Component { 

  render() {
  clusterData(data);
  return (
    <div className="viz-container">
      <br></br>
      <ScatterChart
        width={700}
        height={500}
        margin={{
          top: 20, right: 20, bottom: 20, left: 20,
        }}
      >
        <CartesianGrid />
        <XAxis type="number" dataKey="returns" name="Returns" />
        <YAxis type="number" dataKey="variances" name="Variances" />
        <Tooltip content={CustomToolTip} />
        <Legend />
        <Scatter name="Group 0" data={group0} fill="red" />
        <Scatter name="Group 1" data={group1} fill="orange" />
        <Scatter name="Group 2" data={group2} fill="green" />
        <Scatter name="Group 3" data={group3}fill="blue" />
        <Scatter name="Group 4" data={group4} fill="purple" />
      </ScatterChart>
    </div>
  );
  }
}

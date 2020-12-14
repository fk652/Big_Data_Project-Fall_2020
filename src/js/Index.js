import '../css/main.css';
import { AreaChart, Area, XAxis, YAxis, Tooltip, Legend } from 'recharts';
import {React, Component} from 'react';
import aggregate_data from '../datasets/S&P_500_Aggregate_Join_Oxford_JohnHopkins_News.json';

const formatNews = (arr) => {
  const headlines = arr.map((line) =>
    <p>{line}</p>
  );
  return (
    <div className="news-tooltip">{headlines}</div>
  );
};

const fixData = (aggregate_data) => {
  let i = 0;
  for (i=0; i<aggregate_data.length; i++){
    if (aggregate_data[i]["Close"] === null){
      aggregate_data[i]["Close"] = aggregate_data[i-1]["Close"]
    }
  }
}

const getNews = (label) => {
  let i = 0;
  for (i=0; i<aggregate_data.length; i++){
    if (aggregate_data[i]["Date"] === label ){
        let news_variable = aggregate_data[i]["News title--source"];
        let close = aggregate_data[i]["Close"];
        let temp = news_variable.split("\t");
        let j = 0;
        let arr = []
        arr.push("S&P Index Close Price: \n"+close);
        for (j=0; j<4; j++){
            arr.push(temp[j])
        }
        let formatted_news = formatNews(arr);
        return formatted_news;
    }
  }
};

const CustomToolTip = ({ active, payload, label }) => {
  if (active) {
    return (
      <div className="custom-tooltip">
        <p className="label">{`${label}`}</p>
        <p className="intro">{getNews(label)}</p>
      </div>
    );
  }

  return null;
};

export default class Index extends Component { 
  render() {
  fixData(aggregate_data);
  return (
    <div className="viz-container">
      <br></br>
      <AreaChart width={750} height={500} data={aggregate_data}
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
        <Tooltip content={<CustomToolTip/>}/>
        <Area type="monotone" dataKey="Close" stroke="#82ca9d" fillOpacity={1} fill="url(#colorPv)" />
      </AreaChart>
    </div>
  );
}
}
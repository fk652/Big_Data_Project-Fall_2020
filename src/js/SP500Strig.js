import { LineChart, Line, XAxis, YAxis, Tooltip, Legend } from 'recharts';
import '../css/main.css';
import {React, Component} from 'react';
import data from '../datasets/S&P_500_Aggregate_Join_Oxford_JohnHopkins_News.json';

const formatNews = (arr) => {
    const headlines = arr.map((line) =>
      <p>{line}</p>
    );
    return (
      <div className="news-tooltip">{headlines}</div>
    );
  };
  
  // const fixData = (data) => {
  //   let i = 0;
  //   for (i=0; i<data.length; i++){
  //     if (data[i]["Close"] === null){
  //       data[i]["Close"] = data[i-1]["Close"]
  //     }
  //   }
  // }
  
  const getNews = (label) => {
    let i = 0;
    for (i=0; i<data.length; i++){
      if (data[i]["Date"] === label ){
          let news_variable = data[i]["News title--source"];
          let close = data[i]["Close_LogChange"];
          let covid = data[i]["USA_StringencyIndex_LogChange"];
          let temp = news_variable.split("\t");
          let j = 0;
          let arr = []
          arr.push("Close Log Change: "+close);
          arr.push("USA Strigency Index_LogChange: "+covid);
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
          <hr></hr>
          <p className="intro">{getNews(label)}</p>
        </div>
      );
    }
  
    return null;
  };

export default class SP500Strig extends Component {

  render() {
    return (
      <LineChart
        width={1000}
        height={600}
        data={data}
        margin={{
          top: 5, right: 30, left: 20, bottom: 5,
        }}
      >
        <XAxis dataKey="Date" />
        <YAxis />
        <Tooltip content={<CustomToolTip/>}/>
        <Legend />
        <Line connectNulls type="monotone" dataKey="Close_LogChange" stroke="#8884d8" dot={false} />
        <Line connectNulls type="monotone" dataKey="USA_StringencyIndex_LogChange" stroke="#82ca9d" dot={false} />
      </LineChart>
    );
  }
}
import '../css/main.css';
import * as d3 from "d3";
import {React, Component} from 'react';
import Sectors from './Sectors';
import Index from './Index';
import Sector_Filter from './Sector_Filter';
import Aggregate from './Aggregate';
import Aggregate_Gains_Drops from './Aggregate_Gains_Drops';
import ConsumerDiscretionary from './ConsumerDiscretionary';
import DualAxis from './DualAxis';
import SP500_Strig from './SP500_Strig';
import Strig_Covid from './Strig_Covid';

export default class Container extends Component { 

  render() {
  return (
    <div className="main-container">
      <h3>S&P 500 Index Interactive Line Graph with News</h3>
      <Index />
      <hr className="hr-div"></hr>
      <h3>S&P Sectors Interactive Line Graph with Dropdown Filter</h3>
      <Sector_Filter />
      <hr className="hr-div"></hr>
      <h3>S&P Aggregate Daily Log Changes (Interactive Positive Negative Bar Chart)</h3>
      <Aggregate />
      <hr className="hr-div"></hr>
      <h3>S&P Aggregate Top 5 Gains and Drops (Interactive Positive Negative Bar Chart)</h3>
      <Aggregate_Gains_Drops />
      <hr className="hr-div"></hr>
      <h3>Consumer Discretionary Sector Clusters (Interactive Scatterplot)</h3>
      <ConsumerDiscretionary />
      <hr className="hr-div"></hr>
      <h3>S&P 500 Index Close and COVID Daily Cases</h3>
      <DualAxis />
      <hr className="hr-div"></hr>
      <h3>S&P 500 Log Change & Strigency Log Change</h3>
      <SP500_Strig />
      <hr className="hr-div"></hr>
      <h3>Strigency Log Change Cumulative and USA Covid Cases Log Change Cumulative </h3>
      <Strig_Covid />
    </div>
  );
  }
}
import '../css/main.css';
import {React, Component} from 'react';
import Landing from './Landing';
import Index from './Index';
import SectorFilter from './SectorFilter';
import Aggregate from './Aggregate';
import AggregateGainsDrops from './AggregateGainsDrops';
import ConsumerDiscretionary from './ConsumerDiscretionary';
import DualAxis from './DualAxis';
import SP500Strig from './SP500Strig';
import StrigCovid from './StrigCovid';
import CommunicationServices from './CommunicationServices';
import FinancialServices from './FinancialServices';
import HealthCare from './HealthCare';
import InformationTechnology from './InformationTechnology';

export default class Container extends Component { 

  render() {
  return (
    <div className="main-container">
      <Landing />
      <hr></hr>
      <h3>S&P 500 Index Interactive Line Graph with News</h3>
      <Index />
      <hr className="hr-div"></hr>
      <h3>S&P Sectors Interactive Line Graph with Dropdown Filter</h3>
      <SectorFilter />
      <hr className="hr-div"></hr>
      <h3>S&P Aggregate Daily Log Changes (Interactive Positive Negative Bar Chart)</h3>
      <Aggregate />
      <hr className="hr-div"></hr>
      <h3>S&P Aggregate Top 5 Gains and Drops (Interactive Positive Negative Bar Chart)</h3>
      <AggregateGainsDrops />
      <hr className="hr-div"></hr>
      <h3>Consumer Discretionary Clusters (Interactive Scatterplot)</h3>
      <ConsumerDiscretionary />
      <hr className="hr-div"></hr>
      <h3>Communication Services Clusters (Interactive Scatterplot)</h3>
      <CommunicationServices />
      <hr className="hr-div"></hr>
      <h3>Financial Services Clusters (Interactive Scatterplot)</h3>
      <FinancialServices />
      <hr className="hr-div"></hr>
      <h3>Information Technology Clusters (Interactive Scatterplot)</h3>
      <InformationTechnology />
      <hr className="hr-div"></hr>
      <h3>Health Care Clusters (Interactive Scatterplot)</h3>
      <HealthCare />
      <hr className="hr-div"></hr>
      <h3>S&P 500 Index Close and COVID Daily Cases</h3>
      <DualAxis />
      <hr className="hr-div"></hr>
      <h3>S&P 500 Log Change & Stringency Log Change</h3>
      <SP500Strig />
      <hr className="hr-div"></hr>
      <h3>Stringency Log Change Cumulative and USA Covid Cases Log Change Cumulative </h3>
      <StrigCovid />
    </div>
  );
  }
}
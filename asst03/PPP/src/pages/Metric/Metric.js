import React from 'react'
import PropTypes from 'prop-types'
import NotFound from '../NotFound/NotFound'
import Chart from "react-google-charts";
import axios from 'axios'
import * as Common from '../../Common';
import InputLabel from "@material-ui/core/InputLabel"
import MenuItem from "@material-ui/core/MenuItem"
import Select from "@material-ui/core/Select"
import Button from "@material-ui/core/Button"
import {withRouter} from 'react-router-dom';

const data = [
  ["Info", "House","Unit"],
  ["Room", 10, 9],
  ["Bath", 14, 7],
  ["Carpark", 5, 6],
];
var options = {
  chart: {
    title: 'Suburb Request Average',
  },
  bars: 'vertical' // Required for Material Bar Charts.
};


class Metric extends React.Component {

  static displayName = 'Metric';

  constructor(props) {
    super(props)
    this.state = {
      login: false,
      init: false,
      suburbChosen: '',
    }
  }


  async componentDidMount() {
    const res = await axios.get(Common.BACKEND_URL + '/trendRecord');

    var admin = JSON.parse(localStorage.getItem("admin"))

    if (this.props.location.state !== undefined && this.props.location.state.login === true || admin != null) {
      this.setState({
        login: true,
        init: true,
        data: res.data.data,
        options: res.data.options,
        suburb: res.data.suburb,
        suburbList: res.data.suburbList
      })
    } else {
      this.setState({
        init: true,
      })
    }
  }

  onChange(e) {
    const sub = e.target.value.toLowerCase()
    // console.log(e.target.value.toLowerCase(),'...');
    var houseRoom = 0;
    var houseBath = 0;
    var houseCarpark = 0;
    var unitRoom = 0;
    var unitBath = 0;
    var unitCarpark = 0;
    if ('room' in this.state.suburb[sub].house) {
      houseRoom = this.state.suburb[sub].house.room;
      houseBath = this.state.suburb[sub].house.bath;
      houseCarpark = this.state.suburb[sub].house.carpark;
    }

    if ('room' in this.state.suburb[sub].unit) {
      unitRoom = this.state.suburb[sub].unit.room;
      unitBath = this.state.suburb[sub].unit.bath;
      unitCarpark = this.state.suburb[sub].unit.carpark;
    }

    this.setState({
      [e.target.name]: e.target.value,
      barChartData: [
        ["Info", "House","Unit"],
        ["Room", houseRoom, unitRoom],
        ["Bath", houseBath, unitBath],
        ["Carpark", houseCarpark, unitCarpark],
      ]
     })
  }

  logout() {
    window.localStorage.clear()
    this.props.history.push('/')
  }

  back() {
    this.props.history.push('/')
  }

  render () {
    if (this.state.login === true && this.state.init === true) {

      var menu = []

      this.state.suburbList.map((s) => {
        menu.push(<MenuItem key={s} value={s}>{s}</MenuItem>)
        return 1;
      })

      return (
        <div className="container">
          <Button
          onClick={this.back.bind(this)}
          variant="contained"
          color="secondary"
          style={{marginTop: '60px'}}
          >
          Main page
          </Button>
          <Button
          onClick={this.logout.bind(this)}
          variant="contained"
          color="primary"
          style={{marginTop: '60px'}}
          >
          Log out
          </Button>
          <Chart
          chartType="PieChart"
          width="100%"
          height="400px"
          data={this.state.data}
          options={this.state.options}
          />
        <div className="row">
          <InputLabel style={{margin: '20px'}}htmlFor="suburb-type">Select Suburb</InputLabel>
          <Select
            value={this.state.suburbChosen}
            onChange={this.onChange.bind(this)}
            inputProps={{
              name: "suburbChosen",
              id: "suburb-type"
            }}
          >
              {menu}
            </Select>
          </div>
          {this.state.suburbChosen.length > 0 ?
            <div>
              <h1>{this.state.suburbChosen} request average</h1>
              <Chart chartType="BarChart" width="100%" height="400px" data={this.state.barChartData}/>
            </div>
            : null
          }

        </div>
      )
    } else if (this.state.init === false) {
      return (
        <h1>loading...</h1>
      )
    } else {
      return (
        <NotFound/>
      )
    }

  }
}

export default withRouter(Metric);

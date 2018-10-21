import React, { Component } from 'react';
import './BasicFilter.scss';
import * as Common from '../../Common';
import { Snackbar, CircularProgress, Select, MenuItem } from '@material-ui/core';
import axios from 'axios';

export default class BasicFilter extends Component {
  static displayName = 'BasicFilter';

  constructor(props) {
    super(props);
    this.state = {
      filter: null,
      isSnackBarOpen: false,
      snackBarMsg: null,
      selectedBedrooms: '',
      selectedBathrooms: '',
      selectedCarSpaces: '',
      selectedType: '',
    }
  }

  componentDidMount() {
    axios.get(Common.BACKEND_URL + '/basicfilters')
      .then((response) => this.setState({ filter: response.data.data }))
      .catch((e) => {
        let errorMsg;
        if (e.response && e.response.data) {
          console.error(e.response);
          errorMsg = e.response.data['msg'];
        } else {
          console.error(e);
          errorMsg = 'Opps! Unknow error happens...';
        }
        this.setState({
          // filter: {
          //   "max_bedroom": 31,
          //   "max_bathroom": 5,
          //   "max_carspace": 3,
          //   "types": [
          //     "house",
          //     "unit",
          //   ]
          // },
          isSnackBarOpen: true,
          snackBarMsg: errorMsg,
        });
      });
  }

  handleChange = event => {
    this.setState({ [event.target.name]: event.target.value });
    if (this.props.onChange) {
      this.props.onChange(event.target.name, event.target.value);
    }
  };

  render() {
    return (
      <div className='basic-filter-container'
        style={this.props.style}
      >
        <Snackbar
          anchorOrigin={{ vertical: 'top', horizontal: 'center' }}
          autoHideDuration={1500}
          open={this.state.isSnackBarOpen}
          message={this.state.snackBarMsg}
          onClose={() => {
            this.setState({
              isSnackBarOpen: false,
              snackBarMsg: null,
            });
          }}
        />
        {this.state.filter ?
          <div className='basic-filters'>
            <div className='basic-filter'>
              <div className='basic-filter-title'>
                Bedroom
              </div>
              <Select
                autoWidth
                style={{ minWidth: '100px' }}
                value={this.state.selectedBedrooms}
                onChange={this.handleChange}
                inputProps={{ name: 'selectedBedrooms' }}
              >
                {
                  Array.apply(null, { length: this.state.filter.max_bedroom }).map(Function.call, Number).map(
                    (i) => {
                      let num = i + 1;
                      return (
                        <MenuItem
                          key={num}
                          value={num}
                        >
                          {num > 1 ? num + ' bedrooms' : num + ' bedroom'}
                        </MenuItem>
                      );
                    }
                  )
                }
              </Select>
            </div>
            <div className='basic-filter'>
              <div className='basic-filter-title'>
                Bathroom
              </div>
              <Select
                autoWidth
                style={{ minWidth: '100px' }}
                value={this.state.selectedBathrooms}
                onChange={this.handleChange}
                inputProps={{ name: 'selectedBathrooms' }}
              >
                {
                  Array.apply(null, { length: this.state.filter.max_bathroom }).map(Function.call, Number).map(
                    (i) => {
                      let num = i + 1;
                      return (
                        <MenuItem
                          key={num}
                          value={num}
                        >
                          {num > 1 ? num + ' bathrooms' : num + ' bathroom'}
                        </MenuItem>
                      );
                    }
                  )
                }
              </Select>
            </div>
            <div className='basic-filter'>
              <div className='basic-filter-title'>
                Car Space
              </div>
              <Select
                autoWidth
                style={{ minWidth: '100px' }}
                value={this.state.selectedCarSpaces}
                onChange={this.handleChange}
                inputProps={{ name: 'selectedCarSpaces' }}
              >
                {
                  Array.apply(null, { length: this.state.filter.max_carspace }).map(Function.call, Number).map(
                    (i) => {
                      let num = i + 1;
                      return (
                        <MenuItem
                          key={num}
                          value={num}
                        >
                          {num > 1 ? num + ' car spaces' : num + ' car space'}
                        </MenuItem>
                      );
                    }
                  )
                }
              </Select>
            </div>
            <div className='basic-filter'>
              <div className='basic-filter-title'>
                Property Type
              </div>
              <Select
                autoWidth
                style={{ minWidth: '100px' }}
                value={this.state.selectedType}
                onChange={this.handleChange}
                inputProps={{ name: 'selectedType' }}
              >
                {
                  this.state.filter.types.map(
                    (type) => {
                      return (
                        <MenuItem
                          key={type}
                          value={type}
                        >
                          {type}
                        </MenuItem>
                      );
                    }
                  )
                }
              </Select>
            </div>
          </div>
          :
          <CircularProgress size={50} />
        }
      </div>
    );
  }
}
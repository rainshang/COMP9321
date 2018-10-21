import React, { Component } from 'react';
import './Predict.scss';
import * as Common from '../../Common';
import Slogon from '../../components/Slogon';
import { CircularProgress, Snackbar, GridList, List, Button} from '@material-ui/core';
import axios from 'axios';
import PPPMap from '../../components/Map/PPPMap';
import { MapProvider } from "../../components/Map/MapContext";
import uuid from 'uuid';

export default class Home extends Component {
  static displayName = 'Home';

  constructor(props) {
    super(props);
    this.data = JSON.parse(decodeURIComponent(escape(atob(this.props.match.params.data))));
    this.state = {};
  }

  componentDidMount() {
    axios.get(Common.BACKEND_URL + '/predictPrice',
      { params: { ...this.data } }
    )
      .then((response) => {
        this.setState({ data: response.data.data });
      })
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
          isSnackBarOpen: true,
          snackBarMsg: errorMsg,
        });
      });
  }

  getDisplayPrice(price) {
    return price.toFixed(1).replace(/\d(?=(\d{3})+\.)/g, '$&,').slice(0, -2);
  }

  render() {
    return (
      <MapProvider>
        <div className='predict-page'>
          <Slogon
            style={{
              margin: 'auto',
            }}
          />
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
          {
            this.state.data ?
              <div>
                <div className='predict-title'>
                  Price Prediction
              </div>
                <div>
                  <div className='predict-content'>
                    <div className='predict-content-box'>
                      <div className='predict-price'>
                        ${this.getDisplayPrice(this.state.data.prediction.main.price)}
                      </div>
                      <div className='filter-info'>
                        For a {this.data.bedroom} bed, {this.data.bathroom} bath, {this.data.carpark} car {this.data.type} in {this.data.suburb}
                      </div>
                    </div>
                  </div>
                </div>
                <div className='section-margin' />
                <div className='section-title'>
                  Mapview
              </div>
                <PPPMap data={this.state.data} />
                <div className='section-title'>
                  Nearby Restaurants
              </div>
                <GridList
                  spacing={80}
                  style={{ height: '370px' }}
                  cols={4}
                >
                  {
                    this.state.data.restaurant.map(item => (
                      <div key={uuid.v4()} className='grid-item'>
                        <img className='grid-img'
                          src={item.photo} />
                        <div>{item.name}</div>
                        <div className='grid-addr'>
                          {item.vicinity}
                        </div>
                        <div className='grid-rating'>
                          {(() => {
                            let rating = '';
                            for (let i = 1; i < item.rating; i++) {
                              rating += '★';
                            }
                            if (!Number.isInteger(item.rating)) {
                              rating += '☆';
                            }
                            return rating;
                          })()}
                        </div>
                      </div>
                    ))
                  }
                </GridList>
                <div className='section-title'>
                  Nearby Schools
              </div>
                <List
                  style={{
                    marginTop: '-30px',
                    marginBottom: '-50px',
                  }}
                >
                  {
                    this.state.data.supermarket.map(item => (
                      <div key={uuid.v4()} className='school-item'>
                        <div className='school-name'>{item.name}</div>
                        <div className='school-rating'>{item.rating}/5</div>
                        <div className='school-addr'>{item.vicinity}</div>
                      </div>
                    ))
                  }
                </List>
                <div className='section-title'>
                  Nearby Hospitals
              </div>
                <List
                  style={{
                    marginTop: '-30px',
                    marginBottom: '-50px',
                  }}
                >
                  {
                    this.state.data.hospital.map(item => (
                      <div key={uuid.v4()} className='hospital-item'>
                        <div className='hospital-name'>{item.name}</div>
                        <div className='hospital-addr'>{item.vicinity}</div>
                      </div>
                    ))
                  }
                </List>
                <div className='section-title'>
                  Nearby Supermarkets
              </div>
                <GridList
                  spacing={80}
                  style={{ height: '370px' }}
                  cols={4}
                >
                  {
                    this.state.data.supermarket.map(item => (
                      <div key={uuid.v4()} className='grid-item'>
                        <img className='grid-img'
                          src={item.photo} />
                        <div>{item.name}</div>
                        <div className='grid-addr'>
                          {item.vicinity}
                        </div>
                        <div className='grid-rating'>
                          {(() => {
                            let rating = '';
                            for (let i = 1; i < item.rating; i++) {
                              rating += '★';
                            }
                            if (!Number.isInteger(item.rating)) {
                              rating += '☆';
                            }
                            return rating;
                          })()}
                        </div>
                      </div>
                    ))
                  }
                </GridList>
                <div className='section-title'
                  style={{ backgroundColor: 'chocolate' }}
                >
                  Other Suggested Areas
              </div>
                <GridList
                  spacing={80}
                  style={{ height: '370px', flexWrap: 'nowrap' }}
                >
                  {
                    this.state.data.prediction.recommendation.map(item => (
                      <div key={uuid.v4()} className='grid-item'>
                        <img className='grid-img'
                          src={item.photo} />
                        <div>{item.suburb}</div>
                        <div className='grid-addr'>
                          ${this.getDisplayPrice(item.price)}
                        </div>
                      </div>
                    ))
                  }
                </GridList>
              </div>
              :
              <CircularProgress
                size={50}
                style={{
                  display: 'table',
                  margin: 'auto',
                }}
              />
          }
        </div>
      </MapProvider>
    );
  }
}

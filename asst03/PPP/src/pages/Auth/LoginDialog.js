import React from 'react'
import PropTypes from 'prop-types'
import Button from "@material-ui/core/Button"
import TextField from "@material-ui/core/TextField"
import Dialog from "@material-ui/core/Dialog"
import Avatar from "@material-ui/core/Avatar"
import FormControl from "@material-ui/core/FormControl"
import LockIcon from "@material-ui/icons/LockOutlined"
import Paper from "@material-ui/core/Paper"
import Typography from "@material-ui/core/Typography"
import withStyles from "@material-ui/core/styles/withStyles"
import Divider from "@material-ui/core/Divider"
import CloseIcon from "@material-ui/icons/Close"
import axios from "axios"
import * as Common from '../../Common';
import {withRouter} from 'react-router-dom';

const styles = theme => ({
  layout: {
    width: "auto",
    display: "block", // Fix IE11 issue.
    marginLeft: theme.spacing.unit,
    marginRight: theme.spacing.unit,
    [theme.breakpoints.up(400 + theme.spacing.unit * 3 * 2)]: {
      width: 600,
      marginLeft: "auto",
      marginRight: "auto"
    }
  },
  paper: {
    display: "flex",
    flexDirection: "column",
    alignItems: "center",
    padding: `${theme.spacing.unit * 2}px ${theme.spacing.unit * 3}px ${theme
      .spacing.unit * 3}px`
  },
  avatar: {
    margin: theme.spacing.unit,
    backgroundColor: theme.palette.secondary.main,
    width: 60,
    height: 60
  },
  form: {
    width: "100%", // Fix IE11 issue.
    marginTop: theme.spacing.unit
  },
  submit: {
    marginTop: theme.spacing.unit * 3
  },
  typo: {
    marginBottom: theme.spacing.unit * 3,
    marginLeft: theme.spacing.unit * 3,
    marginRight: theme.spacing.unit * 3,
    marginTop: theme.spacing.unit * 3
  },
  button: {
    margin: theme.spacing.unit * 3
  }
})

class LoginDialog extends React.Component {

  constructor(props) {
    super(props);
    this.state = {
      username: '',
      password: '',
      error: {
        status: false,
        message: 'wrong username or password'
      }
    }
  }

  onChange(e) {
    this.setState({
      [e.target.name]: e.target.value,
      error: {
        status: false,
        message: ''
      }
    })
  }

  onClose = () => {
    this.props.closeDialog();
  }

  async handleLogin(e) {
    e.preventDefault();
    const user = {
      username: this.state.username,
      password: this.state.password
    }
    const res = await axios.post(Common.BACKEND_URL + '/login',user);

    if (res.data.login === false) {
      this.setState({
        error: {
          status: true,
          message: res.data.msg
        }
      })
    } else {
      this.props.closeDialog();
      this.props.history.push({
        pathname: `/metric`,
        state: {
          login: true
        }
      });
      const localstorage = {
        "loggin": true
      }
      localStorage.setItem("admin", JSON.stringify(localstorage))
    }

  }


  render () {
    const {classes} = this.props;

    return (
      <Dialog
        open={true}
        aria-labelledby="form-dialog-title"
        disableBackdropClick={true}
        className={classes.layout}
      >
        <CloseIcon
          style={{ float: "right" }}
          onClick={this.onClose}
        />
        <Paper className={classes.paper}>
          <Avatar className={classes.avatar}>
            <LockIcon />
          </Avatar>
          <Typography variant="h6">Authentication</Typography>
          <form
            className={classes.form}
            onSubmit={this.handleLogin.bind(this)}
          >
              <div>
                <FormControl margin="normal" required fullWidth>
                  <TextField
                    error={this.state.error.status}
                    helperText={
                            this.state.error.status === true
                              ? this.state.error.message
                              : null
                          }
                    label="Username"
                    autoFocus
                    id="username"
                    name="username"
                    type="text"
                    autoComplete="username"
                    onChange={this.onChange.bind(this)}
                  />
                </FormControl>
                <FormControl margin="normal" required fullWidth>
                  <TextField
                    error={this.state.error.status}
                    helperText={
                            this.state.error.status === true
                              ? this.state.error.message
                              : null
                          }
                    label="Password"
                    name="password"
                    type="password"
                    id="password"
                    autoComplete="current-password"
                    onChange={this.onChange.bind(this)}
                  />
                </FormControl>
                <Button
                  type="submit"
                  fullWidth
                  variant="contained"
                  color="primary"
                  style={{marginTop: '60px'}}
                >
                  Sign in
                </Button>
              </div>
          </form>
        </Paper>
      </Dialog>
    )
  }
}

export default withRouter(withStyles(styles)(LoginDialog))

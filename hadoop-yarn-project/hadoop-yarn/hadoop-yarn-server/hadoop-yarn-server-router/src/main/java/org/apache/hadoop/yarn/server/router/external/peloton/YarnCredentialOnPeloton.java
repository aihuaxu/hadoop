package org.apache.hadoop.yarn.server.router.external.peloton;

public class YarnCredentialOnPeloton {
  String username;
  String password;

  public YarnCredentialOnPeloton() {
  }

  public YarnCredentialOnPeloton(String user, String password) {
    this.username = user;
    this.password = password;
  }

  protected String getUsername() {
    return username;
  }

  public void setUsername(String username) {
    this.username = username;
  }

  protected String getPassword() {
    return password;
  }

  public void setPassword(String password) {
    this.password = password;
  }
}

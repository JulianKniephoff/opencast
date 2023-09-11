package org.opencastproject.security.jwt;

import org.opencastproject.security.api.SecurityService;
import org.opencastproject.security.impl.jpa.JpaOrganization;
import org.opencastproject.security.impl.jpa.JpaRole;
import org.opencastproject.security.impl.jpa.JpaUserReference;
import org.opencastproject.userdirectory.api.UserReferenceProvider;
import org.springframework.web.filter.GenericFilterBean;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import java.io.IOException;
import java.util.Date;
import java.util.Set;

public class TestFilter extends GenericFilterBean {
  private SecurityService securityService;
  private UserReferenceProvider userReferenceProvider;

  @Override
  public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain filterChain) throws IOException, ServletException {
    var organization = securityService.getOrganization();
    var jpaOrganization = new JpaOrganization(organization.getId(), organization.getName(), organization.getServers(),
            organization.getAdminRole(), organization.getAdminRole(), organization.getProperties());

    var userReference = userReferenceProvider.findUserReference("foo", "mh_default_org");
    if (userReference == null) {
      userReference = new JpaUserReference("foo", "John Doe", "jdoe@example.com", "test", new Date(), jpaOrganization);
      userReferenceProvider.addUserReference(userReference, "test");
    } else {
      userReference.setRoles(Set.of(new JpaRole("ROLE_FOO", jpaOrganization)));
      userReferenceProvider.updateUserReference(userReference);
    }
    filterChain.doFilter(servletRequest, servletResponse);
  }

  public void setSecurityService(SecurityService securityService) {
    this.securityService = securityService;
  }

  public void setUserReferenceProvider(UserReferenceProvider userReferenceProvider) {
    this.userReferenceProvider = userReferenceProvider;
  }
}

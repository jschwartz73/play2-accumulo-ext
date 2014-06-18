package com.schwartech.accumulo.ext.helpers;

import com.schwartech.accumulo.Accumulo;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.security.Authorizations;
import play.Logger;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by jeff on 3/24/14.
 */
public class UserHelper {

    public static boolean userExists(String username) throws AccumuloSecurityException, AccumuloException {
        boolean userExists = false;

        Connector connector = null;

        try {
            connector = Accumulo.getConnector();
            userExists = connector.securityOperations().listLocalUsers().contains(username);
        } finally {
            Accumulo.releaseConnector(connector);
        }

        return userExists;
    }

    public static boolean validateUser(String username, String password) {
        boolean success = false;

        Connector connector = null;

        try {
            connector = Accumulo.getConnector();
            PasswordToken token = new PasswordToken(password.getBytes());
            success = connector.securityOperations().authenticateUser(username, token);
        } catch (Exception e) {
            Logger.error("Error validating user", e);
        } finally {
            Accumulo.releaseConnector(connector);
        }

        return success;
    }

    public static void createUser(String username, String password) throws AccumuloSecurityException, AccumuloException {
        PasswordToken token = new PasswordToken(password.getBytes());

        if(!userExists(username)) {
            Connector connector = null;

            try {
                connector = Accumulo.getConnector();
                connector.securityOperations().createLocalUser(username, token);
                Logger.debug("User created: " + username);
            } finally {
                Accumulo.releaseConnector(connector);
            }
        }
    }

    public static void deleteUser(String username) throws AccumuloSecurityException, AccumuloException {
        Connector connector = null;
        try {
            connector = Accumulo.getConnector();
            connector.securityOperations().dropLocalUser(username);
            Logger.debug("User deleted: " + username);
        } finally {
            Accumulo.releaseConnector(connector);
        }
    }

    public static Set<String> getUsernames() throws AccumuloSecurityException, AccumuloException {
        Set<String> usernames = new HashSet<String>();

        Connector connector = null;

        try {
            connector = Accumulo.getConnector();
            usernames = connector.securityOperations().listLocalUsers();
        } finally {
            Accumulo.releaseConnector(connector);
        }

        return usernames;
    }

    public static void removeRole(String username, String role) throws AccumuloSecurityException, AccumuloException {
        Set<byte[]> roles = new HashSet(getRolesAsSet(username));

        roles.remove(role.getBytes());

        saveUpdateRoles(username, roles);
    }

    private static void saveUpdateRoles(String username, Set<byte[]> roles) throws AccumuloSecurityException, AccumuloException {
        Authorizations newAuths = new Authorizations(roles);

        Connector connector = null;

        try {
            connector = Accumulo.getConnector();
            connector.securityOperations().changeUserAuthorizations(username, newAuths);
        } finally {
            Accumulo.releaseConnector(connector);
        }
    }

    public static void addRole(String username, String role) throws AccumuloSecurityException, AccumuloException {
        Set<byte[]> roles = new HashSet(getRolesAsSet(username));
        roles.add(role.getBytes());

        saveUpdateRoles(username, roles);
    }

    public static Authorizations getAuths(String username) throws AccumuloSecurityException, AccumuloException {
        Authorizations auths = new Authorizations();

        Connector connector = null;

        try {
            connector = Accumulo.getConnector();
            auths = connector.securityOperations().getUserAuthorizations(username);
        } finally {
            Accumulo.releaseConnector(connector);
        }

        return auths;
    }

    public static Set<byte[]> getRolesAsSet(String username) throws AccumuloSecurityException, AccumuloException {
        Authorizations auths = getAuths(username);
        Set<byte[]> roles = new HashSet<byte[]>();
        for (byte[] role : auths) {
            roles.add(role);
        }
        return roles;
    }
}
package mflix.api.daos;

import com.mongodb.MongoClientSettings;
import com.mongodb.MongoWriteException;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.DeleteResult;
import mflix.api.models.Session;
import mflix.api.models.User;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

import java.util.Map;
import java.util.Objects;

import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

@Configuration
public class UserDao extends AbstractMFlixDao {

    private final MongoCollection<User> usersCollection;
    private final MongoCollection<Session> sessionsCollection;

    private final Logger log;

    @Autowired
    public UserDao(
            MongoClient mongoClient, @Value("${spring.mongodb.database}") String databaseName) {
        super(mongoClient, databaseName);
        CodecRegistry pojoCodecRegistry =
                fromRegistries(
                        MongoClientSettings.getDefaultCodecRegistry(),
                        fromProviders(PojoCodecProvider.builder().automatic(true).build()));

        usersCollection = db.getCollection("users", User.class).withCodecRegistry(pojoCodecRegistry);
        log = LoggerFactory.getLogger(this.getClass());
        sessionsCollection = db.getCollection("sessions", Session.class).withCodecRegistry(pojoCodecRegistry);
    }

    /**
     * Inserts the `user` object in the `users` collection.
     *
     * @param user - User object to be added
     * @return True if successful, throw IncorrectDaoOperation otherwise
     */
    public boolean addUser(User user) {
        try {
            if (!userExists(user.getEmail())) {
                throw new RuntimeException("User with email " + user.getEmail() + " already exists");
            }
            usersCollection.insertOne(user);
            log.info("User with email {} has successfully been added", user.getEmail());
            return true;
        } catch (MongoWriteException ex) {
            log.error("Exception happened during adding user", ex);
            throw new IncorrectDaoOperation(ex.getMessage());
        }
    }

    /**
     * Creates session using userId and jwt token.
     *
     * @param userId - user string identifier
     * @param jwt    - jwt string token
     * @return true if successful
     */
    public boolean createUserSession(String userId, String jwt) {
        Bson updateFilter = new Document("user_id", userId);
        Bson setUpdate = Updates.set("jwt", jwt);
        UpdateOptions options = new UpdateOptions().upsert(true);
        try {
            Session session = sessionsCollection.find(updateFilter).first();
            if (Objects.isNull(session)) {
                sessionsCollection.updateOne(updateFilter, setUpdate, options);
                log.info("User session has successfully been crated");
                return true;
            }
            throw new RuntimeException("Session for user with id " + userId + " already exists");
        } catch (MongoWriteException ex) {
            log.error("Exception happened during user session creation", ex);
            throw new IncorrectDaoOperation(ex.getMessage());
        }
    }

    /**
     * Returns the User object matching the an email string value.
     *
     * @param email - email string to be matched.
     * @return User object or null.
     */
    public User getUser(String email) {
        return usersCollection.find(new Document("email", email)).limit(1).first();
    }

    /**
     * Given the userId, returns a Session object.
     *
     * @param userId - user string identifier.
     * @return Session object or null.
     */
    public Session getUserSession(String userId) {
        return sessionsCollection.find(new Document("user_id", userId)).limit(1).first();
    }

    public boolean deleteUserSessions(String userId) {
        Document sessionDeleteFilter = new Document("user_id", userId);
        DeleteResult res = sessionsCollection.deleteOne(sessionDeleteFilter);
        return res.wasAcknowledged();
    }

    /**
     * Removes the user document that match the provided email.
     *
     * @param email - of the user to be deleted.
     * @return true if user successfully removed
     */
    public boolean deleteUser(String email) {
        if (!userExists(email)) {
            throw new RuntimeException("User with email " + email + " doesn't exist");
        }
        try {
            if (deleteUserSessions(email)) {
                Document userDeleteFilter = new Document("email", email);
                DeleteResult res = usersCollection.deleteOne(userDeleteFilter);
                log.info("User with email {} has been deleted successfully", email);
                return res.wasAcknowledged();
            }
        } catch (MongoWriteException ex) {
            log.error("Exception happened during user deletion", ex);
            throw new IncorrectDaoOperation(ex.getMessage());
        }
        return false;
    }

    /**
     * Updates the preferences of an user identified by `email` parameter.
     *
     * @param email           - user to be updated email
     * @param userPreferences - set of preferences that should be stored and replace the existing
     *                        ones. Cannot be set to null value
     * @return User object that just been updated.
     */
    public boolean updateUserPreferences(String email, Map<String, ?> userPreferences) {
        if (Objects.isNull(userPreferences)) {
            throw new IncorrectDaoOperation("User preferences should not be null");
        }
        Bson user = new Document("email", email);
        Bson updateObject = Updates.set("preferences", userPreferences);
        if (userExists(email)) {
            throw new RuntimeException("User with email " + email + " already exists");
        }
        try {
            usersCollection.updateOne(user, updateObject);
            log.info("User preferences have been updated successfully");
            return true;
        } catch (MongoWriteException ex) {
            log.error("Exception happened during user preferences updating", ex);
            throw new IncorrectDaoOperation(ex.getMessage());
        }
    }

    private boolean userExists(String userEmail) {
        Bson filter = Filters.eq(Filters.eq("email", userEmail));
        User userToCheck = usersCollection.find(filter).first();
        return userToCheck != null;
    }

}

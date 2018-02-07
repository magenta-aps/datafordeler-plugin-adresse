package dk.magenta.datafordeler.adresseservice;

import dk.magenta.datafordeler.core.database.InterruptedPullFile;
import dk.magenta.datafordeler.core.database.QueryManager;
import dk.magenta.datafordeler.core.database.SessionManager;
import dk.magenta.datafordeler.core.exception.DataFordelerException;
import dk.magenta.datafordeler.core.exception.HttpNotFoundException;
import dk.magenta.datafordeler.core.exception.InvalidClientInputException;
import dk.magenta.datafordeler.core.exception.MissingParameterException;
import dk.magenta.datafordeler.core.fapi.Query;
import dk.magenta.datafordeler.core.user.DafoUserDetails;
import dk.magenta.datafordeler.core.user.DafoUserManager;
import dk.magenta.datafordeler.core.util.LoggerHelper;
import dk.magenta.datafordeler.gladdrreg.data.address.AddressEntity;
import dk.magenta.datafordeler.gladdrreg.data.address.AddressQuery;
import dk.magenta.datafordeler.gladdrreg.data.locality.LocalityEntity;
import dk.magenta.datafordeler.gladdrreg.data.locality.LocalityQuery;
import dk.magenta.datafordeler.gladdrreg.data.municipality.MunicipalityData;
import dk.magenta.datafordeler.gladdrreg.data.municipality.MunicipalityEffect;
import dk.magenta.datafordeler.gladdrreg.data.municipality.MunicipalityEntity;
import dk.magenta.datafordeler.gladdrreg.data.municipality.MunicipalityRegistration;
import dk.magenta.datafordeler.gladdrreg.data.road.RoadEntity;
import dk.magenta.datafordeler.gladdrreg.data.road.RoadQuery;
import org.hibernate.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.PostConstruct;
import javax.servlet.http.HttpServletRequest;
import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/adresse")
public class AdresseService {

    @Autowired
    SessionManager sessionManager;

    @Autowired
    private DafoUserManager dafoUserManager;

    private Logger log = LoggerFactory.getLogger(AdresseService.class);

    public static final String PARAM_MUNICIPALITY = "kommune";
    public static final String PARAM_LOCALITY = "lokalitet";
    public static final String PARAM_ROAD = "vej";
    public static final String PARAM_HOUSE = "husnr";
    public static final String PARAM_BNR = "bnr";


    HashMap<Integer, UUID> municipalities = new HashMap<>();

    /**
     * Load known municipalities into a local map of municipalityCode: UUID
     */
    @PostConstruct
    public void loadMunicipalities() {
        Session session = sessionManager.getSessionFactory().openSession();
        try {
            List<MunicipalityEntity> municipalities = QueryManager.getAllEntities(session, MunicipalityEntity.class);
            for (MunicipalityEntity municipality : municipalities) {
                MunicipalityData data = getData(municipality);
                if (data != null) {
                    this.municipalities.put(data.getCode(), municipality.getUUID());
                }
            }
        } finally {
            session.close();
        }
    }

    private static MunicipalityData getData(MunicipalityEntity municipality) {
        OffsetDateTime now = OffsetDateTime.now();
        MunicipalityRegistration registration = municipality.getRegistrationAt(now);
        if (registration != null) {
            for (MunicipalityEffect effect : registration.getEffectsAt(now)) {
                for (MunicipalityData data : effect.getDataItems()) {
                    if (data.getCode() != 0) {
                        return data;
                    }
                }
            }
        }
        return null;
    }


    /**
     * Finds all localities in a municipality. Only current data is included.
     * @param request HTTP request containing a municipality parameter
     * @return Json-formatted string containing a list of found objects
     */
    @RequestMapping("/lokalitet")
    public String getLocalities(HttpServletRequest request) throws DataFordelerException {
        String municipalityCode = request.getParameter(PARAM_MUNICIPALITY);
        DafoUserDetails user = dafoUserManager.getUserFromRequest(request);
        LoggerHelper loggerHelper = new LoggerHelper(log, request, user);
        loggerHelper.info(
                "Incoming REST request for AddressService.locality with municipality {}", municipalityCode
        );
        checkParameterExistence(PARAM_MUNICIPALITY, municipalityCode);
        int code = parameterAsInt(PARAM_MUNICIPALITY, municipalityCode);
        UUID municipality = this.municipalities.get(code);
        if (municipality == null) {
            throw new HttpNotFoundException("Municipality with code "+code+" not found");
        }

        LocalityQuery query = new LocalityQuery();
        setQueryNow(query);
        query.setMunicipality(municipality.toString());
        Session session = sessionManager.getSessionFactory().openSession();
        try {
            List<LocalityEntity> localities = QueryManager.getAllEntities(session, query, LocalityEntity.class);
            System.out.println(localities);
        } finally {
            session.close();
        }

        return "";
    }

    /**
     * Finds all roads in a locality. Only current data is included.
     * @param request HTTP request containing a locality parameter
     * @return Json-formatted string containing a list of found objects
     */
    @RequestMapping("/vej")
    public String getRoads(HttpServletRequest request) throws DataFordelerException {
        String localityUUID = request.getParameter(PARAM_LOCALITY);
        DafoUserDetails user = dafoUserManager.getUserFromRequest(request);
        LoggerHelper loggerHelper = new LoggerHelper(log, request, user);
        loggerHelper.info(
                "Incoming REST request for AddressService.road with locality {}", localityUUID
        );
        checkParameterExistence(PARAM_LOCALITY, localityUUID);
        UUID locality = parameterAsUUID(PARAM_LOCALITY, localityUUID);
        RoadQuery query = new RoadQuery();
        setQueryNow(query);
        query.setLocality(locality.toString());
        Session session = sessionManager.getSessionFactory().openSession();
        try {
            List<RoadEntity> roads = QueryManager.getAllEntities(session, query, RoadEntity.class);
            System.out.println(roads);
        } finally {
            session.close();
        }
        return "";
    }

    /**
     * Finds all buildings on a road. Only current data is included.
     * @param request HTTP request containing a road parameter
     * @return Json-formatted string containing a list of found objects
     */
    @RequestMapping("/hus")
    public String getBuildings(HttpServletRequest request) throws DataFordelerException {
        String roadUUID = request.getParameter(PARAM_ROAD);
        DafoUserDetails user = dafoUserManager.getUserFromRequest(request);
        LoggerHelper loggerHelper = new LoggerHelper(log, request, user);
        loggerHelper.info(
                "Incoming REST request for AddressService.building with road {}", roadUUID
        );
        checkParameterExistence(PARAM_ROAD, roadUUID);

        UUID road = parameterAsUUID(PARAM_ROAD, roadUUID);
        AddressQuery query = new AddressQuery();
        setQueryNow(query);
        query.setRoad(road.toString());
        Session session = sessionManager.getSessionFactory().openSession();
        try {
            List<AddressEntity> addresses = QueryManager.getAllEntities(session, query, AddressEntity.class);
            System.out.println(addresses);
        } finally {
            session.close();
        }
        return "";
    }

    /**
     * Finds all addreses on a road, filtered by housenumber or bnumber.
     * Only current data is included.
     * @param request HTTP request containing a road parameter,
     *                and optionally a house parameter or bnr parameter
     * @return Json-formatted string containing a list of found objects
     */
    @RequestMapping("/adresse")
    public String getAddresses(HttpServletRequest request) throws DataFordelerException {
        String roadId = request.getParameter(PARAM_ROAD);
        String houseNumber = request.getParameter(PARAM_HOUSE);
        String buildingNumber = request.getParameter(PARAM_BNR);
        DafoUserDetails user = dafoUserManager.getUserFromRequest(request);
        LoggerHelper loggerHelper = new LoggerHelper(log, request, user);
        loggerHelper.info(
                "Incoming REST request for AddressService.address with road {}, houseNumber {}, bNumber {}", roadId, houseNumber, buildingNumber
        );
        checkParameterExistence(PARAM_ROAD, roadId);
        return "";
    }

    private static void checkParameterExistence(String name, String value) throws MissingParameterException {
        if (value == null || value.trim().isEmpty()) {
            throw new MissingParameterException(name);
        }
    }

    private static int parameterAsInt(String name, String value) throws InvalidClientInputException {
        try {
            return Integer.parseInt(value, 10);
        } catch (NumberFormatException e) {
            throw new InvalidClientInputException("Parameter "+name+" must be a number", e);
        }
    }

    private static UUID parameterAsUUID(String name, String value) throws InvalidClientInputException {
        try {
            return UUID.fromString(value);
        } catch (IllegalArgumentException e) {
            throw new InvalidClientInputException("Parameter "+name+" must be a uuid", e);
        }
    }

    private static void setQueryNow(Query query) {
        OffsetDateTime now = OffsetDateTime.now();
        query.setRegistrationFrom(now);
        query.setRegistrationTo(now);
        query.setEffectFrom(now);
        query.setEffectTo(now);
    }
}

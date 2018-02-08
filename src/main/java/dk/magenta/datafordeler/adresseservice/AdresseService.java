package dk.magenta.datafordeler.adresseservice;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import dk.magenta.datafordeler.core.database.*;
import dk.magenta.datafordeler.core.exception.DataFordelerException;
import dk.magenta.datafordeler.core.exception.HttpNotFoundException;
import dk.magenta.datafordeler.core.exception.InvalidClientInputException;
import dk.magenta.datafordeler.core.exception.MissingParameterException;
import dk.magenta.datafordeler.core.fapi.ParameterMap;
import dk.magenta.datafordeler.core.fapi.Query;
import dk.magenta.datafordeler.core.user.DafoUserDetails;
import dk.magenta.datafordeler.core.user.DafoUserManager;
import dk.magenta.datafordeler.core.util.ListHashMap;
import dk.magenta.datafordeler.core.util.LoggerHelper;
import dk.magenta.datafordeler.gladdrreg.data.address.AddressData;
import dk.magenta.datafordeler.gladdrreg.data.address.AddressEntity;
import dk.magenta.datafordeler.gladdrreg.data.address.AddressQuery;
import dk.magenta.datafordeler.gladdrreg.data.bnumber.BNumberData;
import dk.magenta.datafordeler.gladdrreg.data.bnumber.BNumberEntity;
import dk.magenta.datafordeler.gladdrreg.data.bnumber.BNumberQuery;
import dk.magenta.datafordeler.gladdrreg.data.locality.LocalityData;
import dk.magenta.datafordeler.gladdrreg.data.locality.LocalityEntity;
import dk.magenta.datafordeler.gladdrreg.data.locality.LocalityQuery;
import dk.magenta.datafordeler.gladdrreg.data.municipality.MunicipalityData;
import dk.magenta.datafordeler.gladdrreg.data.municipality.MunicipalityEffect;
import dk.magenta.datafordeler.gladdrreg.data.municipality.MunicipalityEntity;
import dk.magenta.datafordeler.gladdrreg.data.municipality.MunicipalityRegistration;
import dk.magenta.datafordeler.gladdrreg.data.road.RoadData;
import dk.magenta.datafordeler.gladdrreg.data.road.RoadEntity;
import dk.magenta.datafordeler.gladdrreg.data.road.RoadQuery;
import org.hibernate.Session;
import org.opensaml.xml.signature.Q;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.PostConstruct;
import javax.servlet.http.HttpServletRequest;
import java.time.OffsetDateTime;
import java.util.*;

@RestController
@RequestMapping("/adresse")
public class AdresseService {

    @Autowired
    SessionManager sessionManager;

    @Autowired
    private DafoUserManager dafoUserManager;

    @Autowired
    private ObjectMapper objectMapper;

    private Logger log = LoggerFactory.getLogger(AdresseService.class);

    public static final String PARAM_MUNICIPALITY = "kommune";
    public static final String PARAM_LOCALITY = "lokalitet";
    public static final String PARAM_ROAD = "vej";
    public static final String PARAM_HOUSE = "husnr";
    public static final String PARAM_BNR = "bnr";

    public static final String OUTPUT_UUID = "uuid";
    public static final String OUTPUT_NAME = "navn";
    public static final String OUTPUT_ABBREVIATION = "forkortelse";
    public static final String OUTPUT_ROADCODE = "vejkode";
    public static final String OUTPUT_ALTNAME = "andet_navn";
    public static final String OUTPUT_CPRNAME = "cpr_navn";
    public static final String OUTPUT_SHORTNAME = "forkortet_navn";
    public static final String OUTPUT_BNUMBER = "b_nummer";
    public static final String OUTPUT_BCALLNAME = "b_kaldenavn";
    public static final String OUTPUT_HOUSENUMBER = "husnummer";
    public static final String OUTPUT_FLOOR = "etage";
    public static final String OUTPUT_DOOR = "doer";



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
            ArrayNode results = objectMapper.createArrayNode();
            for (LocalityEntity locality : localities) {
                Set<DataItem> dataItems = locality.getCurrent();
                ObjectNode localityNode = objectMapper.createObjectNode();
                localityNode.put(OUTPUT_UUID, locality.getUUID().toString());
                for (DataItem dataItem : dataItems) {
                    LocalityData data = (LocalityData) dataItem;
                    if (data.getName() != null) {
                        localityNode.put(OUTPUT_NAME, data.getName());
                    }
                    if (data.getAbbrev() != null) {
                        localityNode.put(OUTPUT_ABBREVIATION, data.getAbbrev());
                    }
                }
                results.add(localityNode);
            }
            return results.toString();
        } finally {
            session.close();
        }
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
            ArrayNode results = objectMapper.createArrayNode();
            for (RoadEntity road : roads) {
                Set<DataItem> dataItems = road.getCurrent();
                ObjectNode roadNode = objectMapper.createObjectNode();
                roadNode.put(OUTPUT_UUID, road.getUUID().toString());
                for (DataItem dataItem : dataItems) {
                    RoadData data = (RoadData) dataItem;
                    if (data.getCode() != 0) {
                        roadNode.put(OUTPUT_ROADCODE, data.getCode());
                    }
                    if (data.getName() != null) {
                        roadNode.put(OUTPUT_NAME, data.getName());
                    }
                    if (data.getAlternateName() != null) {
                        roadNode.put(OUTPUT_ALTNAME, data.getAlternateName());
                    }
                    if (data.getCprName() != null) {
                        roadNode.put(OUTPUT_CPRNAME, data.getCprName());
                    }
                    if (data.getShortName() != null) {
                        roadNode.put(OUTPUT_SHORTNAME, data.getShortName());
                    }
                }
                results.add(roadNode);
            }
            return results.toString();
        } finally {
            session.close();
        }
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

        Session session = sessionManager.getSessionFactory().openSession();
        try {
            org.hibernate.query.Query<Object[]> query = session.createQuery(
                "SELECT DISTINCT e, b FROM "+AddressData.class.getCanonicalName()+" d " +
                "JOIN d.effects v " +
                "JOIN v.registration r " +
                "JOIN r.entity e " +
                "JOIN d."+AddressData.DB_FIELD_ROAD+" d_road " +
                "JOIN d."+AddressData.DB_FIELD_BNUMBER+" d_bNumber " +
                "JOIN "+BNumberEntity.class.getCanonicalName()+" b ON b.identification = d_bNumber " +
                "WHERE d_road.uuid = :d_road_uuid"
            );
            query.setParameter("d_road_uuid", road);

            ArrayNode results = objectMapper.createArrayNode();
            for (Object[] result : query.getResultList()) {
                AddressEntity addressEntity = (AddressEntity) result[0];
                BNumberEntity bNumberEntity = (BNumberEntity) result[1];
                ObjectNode addressNode = objectMapper.createObjectNode();

                Set<DataItem> addressDataItems = addressEntity.getCurrent();
                Set<DataItem> bNumberDataItems = bNumberEntity.getCurrent();
                for (DataItem dataItem : addressDataItems) {
                    AddressData data = (AddressData) dataItem;
                    if (data.getHouseNumber() != null) {
                        addressNode.put(OUTPUT_HOUSENUMBER, data.getHouseNumber());
                    }
                }
                for (DataItem dataItem : bNumberDataItems) {
                    BNumberData data = (BNumberData) dataItem;
                    if (data.getCode() != null && !data.getCode().isEmpty()) {
                        addressNode.put(OUTPUT_BNUMBER, data.getCode());
                    }
                    if (data.getCallname() != null && !data.getCallname().isEmpty()) {
                        addressNode.put(OUTPUT_BCALLNAME, data.getCallname());
                    }
                }
                results.add(addressNode);
            }
            return results.toString();
        } finally {
            session.close();
        }
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
        String roadUUID = request.getParameter(PARAM_ROAD);
        String houseNumber = request.getParameter(PARAM_HOUSE);
        String buildingNumber = request.getParameter(PARAM_BNR);
        DafoUserDetails user = dafoUserManager.getUserFromRequest(request);
        LoggerHelper loggerHelper = new LoggerHelper(log, request, user);
        loggerHelper.info(
                "Incoming REST request for AddressService.address with road {}, houseNumber {}, bNumber {}", roadUUID, houseNumber, buildingNumber
        );
        checkParameterExistence(PARAM_ROAD, roadUUID);
        UUID road = parameterAsUUID(PARAM_ROAD, roadUUID);

        AddressQuery query = new AddressQuery();
        setQueryNow(query);
        query.setRoad(road.toString());
        if (houseNumber != null && !houseNumber.trim().isEmpty()) {
            query.setHouseNumber(houseNumber.trim());
        }
        if (buildingNumber != null && !buildingNumber.trim().isEmpty()) {
            query.setBnr(buildingNumber);
        }
        Session session = sessionManager.getSessionFactory().openSession();
        try {
            // We only get bnumber references (uuids) here, and must
            // look them up in the bnumber table
            HashSet<UUID> bNumbers = new HashSet<>();
            List<AddressEntity> addressEntities = QueryManager.getAllEntities(session, query, AddressEntity.class);
            ArrayNode results = objectMapper.createArrayNode();
            for (AddressEntity addressEntity : addressEntities) {
                Set<DataItem> addressDataItems = addressEntity.getCurrent();
                for (DataItem dataItem : addressDataItems) {
                    AddressData data = (AddressData) dataItem;
                    if (data.getbNumber() != null) {
                        bNumbers.add(data.getbNumber().getUuid());
                    }
                }
            }

            HashMap<UUID, String> bNumberMap = new HashMap<>();
            org.hibernate.query.Query<Object[]> bQuery = session.createQuery(
                    "SELECT DISTINCT e, e.identification.uuid FROM "+BNumberEntity.class.getCanonicalName()+" e "+
                    "WHERE e.identification.uuid in (:uuids)"
            );
            bQuery.setParameterList("uuids", bNumbers);

            for (Object[] resultItem : bQuery.getResultList()) {
                BNumberEntity bNumberEntity = (BNumberEntity) resultItem[0];
                UUID uuid = (UUID) resultItem[1];
                for (DataItem dataItem : bNumberEntity.getCurrent()) {
                    BNumberData data = (BNumberData) dataItem;
                    if (data.getCode() != null) {
                        bNumberMap.put(uuid, data.getCode());
                        break;
                    }
                }
            }

            for (AddressEntity addressEntity : addressEntities) {
                Set<DataItem> addressDataItems = addressEntity.getCurrent();
                ObjectNode addressNode = objectMapper.createObjectNode();
                addressNode.put(OUTPUT_UUID, addressEntity.getUUID().toString());
                for (DataItem dataItem : addressDataItems) {
                    AddressData data = (AddressData) dataItem;
                    if (data.getHouseNumber() != null) {
                        addressNode.put(OUTPUT_HOUSENUMBER, data.getHouseNumber());
                    }
                    if (data.getFloor() != null && !data.getFloor().isEmpty()) {
                        addressNode.put(OUTPUT_FLOOR, data.getFloor());
                    }
                    if (data.getRoom() != null && !data.getRoom().isEmpty()) {
                        addressNode.put(OUTPUT_DOOR, data.getRoom());
                    }
                    if (data.getbNumber() != null) {
                        String code = bNumberMap.get(data.getbNumber().getUuid());
                        if (code != null) {
                            addressNode.put(OUTPUT_BNUMBER, code);
                        }
                    }
                }
                results.add(addressNode);
            }

            return results.toString();
        } finally {
            session.close();
        }
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

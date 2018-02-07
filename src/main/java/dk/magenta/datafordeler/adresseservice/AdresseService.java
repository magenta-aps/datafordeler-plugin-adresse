package dk.magenta.datafordeler.adresseservice;

import dk.magenta.datafordeler.core.database.SessionManager;
import dk.magenta.datafordeler.core.exception.DataFordelerException;
import dk.magenta.datafordeler.core.exception.MissingParameterException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;

@RestController
@RequestMapping("/adresse")
public class AdresseService {

    @Autowired
    SessionManager sessionManager;

    public static final String PARAM_MUNICIPALITY = "kommune";
    public static final String PARAM_LOCALITY = "lokalitet";
    public static final String PARAM_ROAD = "vej";
    public static final String PARAM_HOUSE = "husnr";
    public static final String PARAM_BNR = "bnr";

    /**
     * Finds all localities in a municipality. Only current data is included.
     * @param request HTTP request containing a municipality parameter
     * @return Json-formatted string containing a list of found objects
     */
    @RequestMapping("/lokalitet")
    public String getLocalities(HttpServletRequest request) throws DataFordelerException {
        System.out.println(request.getParameterMap());
        String municipalityCode = request.getParameter(PARAM_MUNICIPALITY);
        checkParameterExistence(PARAM_MUNICIPALITY, municipalityCode);
        return "";
    }

    /**
     * Finds all roads in a locality. Only current data is included.
     * @param request HTTP request containing a locality parameter
     * @return Json-formatted string containing a list of found objects
     */
    @RequestMapping("/vej")
    public String getRoads(HttpServletRequest request) throws DataFordelerException {
        String localityId = request.getParameter(PARAM_LOCALITY);
        checkParameterExistence(PARAM_LOCALITY, localityId);
        return "";
    }

    /**
     * Finds all buildings on a road. Only current data is included.
     * @param request HTTP request containing a road parameter
     * @return Json-formatted string containing a list of found objects
     */
    @RequestMapping("/hus")
    public String getBuildings(HttpServletRequest request) throws DataFordelerException {
        String roadId = request.getParameter(PARAM_ROAD);
        checkParameterExistence(PARAM_ROAD, roadId);
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
        checkParameterExistence(PARAM_ROAD, roadId);
        String houseNumber = request.getParameter(PARAM_HOUSE);
        String buildingNumber = request.getParameter(PARAM_BNR);
        return "";
    }

    private static void checkParameterExistence(String name, String value) throws MissingParameterException {
        if (value == null || value.trim().isEmpty()) {
            throw new MissingParameterException(name);
        }
    }
}

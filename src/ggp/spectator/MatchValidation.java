package ggp.spectator;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.ggp.base.util.crypto.SignableJSON;
import org.ggp.base.util.symbol.factory.SymbolFactory;
import org.ggp.base.util.symbol.factory.exceptions.SymbolFormatException;
import org.ggp.base.util.symbol.grammar.SymbolList;

import external.JSON.JSONArray;
import external.JSON.JSONException;
import external.JSON.JSONObject;

public class MatchValidation {
    // =======================================================================
    // Validation checks, to make sure bad information is not published to the
    // spectator server by a malicious adversary or malfunctioning system. These
    // are not comprehensive, but are intended to provide basic sanity guarantees
    // so that we aren't storing total nonsense in the spectator server.
    @SuppressWarnings("serial")
    public static class ValidationException extends IOException {
        public ValidationException(String x) {
            super(x);
        }
    }

    public static void performCreationValidationChecks(JSONObject newMatchJSON) throws ValidationException {
        try {
            verifyReasonableTime(newMatchJSON.getLong("startTime"));

            if (newMatchJSON.getString("randomToken").length() < 12) {
                throw new ValidationException("Random token is too short.");
            }

            if (newMatchJSON.has("matchHostPK") && !newMatchJSON.has("matchHostSignature")) {
                throw new ValidationException("Signatures required for matches identified with public keys.");
            }
        } catch(JSONException e) {
            throw new ValidationException("Could not parse JSON: " + e.toString());
        }
    }

    // These fields must remain unchanged when comparing any two versions of the same match.
    public static void performUpdateInvariantValidationChecks(JSONObject oldMatchJSON, JSONObject newMatchJSON) throws ValidationException {
        try {
            verifyEquals(oldMatchJSON, newMatchJSON, "matchId");
            verifyEquals(oldMatchJSON, newMatchJSON, "startTime");
            verifyEquals(oldMatchJSON, newMatchJSON, "randomToken");
            verifyEquals(oldMatchJSON, newMatchJSON, "matchHostPK");
            verifyEquals(oldMatchJSON, newMatchJSON, "startClock");
            verifyEquals(oldMatchJSON, newMatchJSON, "playClock");
            verifyEquals(oldMatchJSON, newMatchJSON, "gameMetaURL");
            verifyEquals(oldMatchJSON, newMatchJSON, "gameName");
            verifyEquals(oldMatchJSON, newMatchJSON, "gameRulesheetHash");
        } catch(JSONException e) {
            throw new ValidationException("Could not parse JSON: " + e.toString());
        }
    }

    // These fields only go in one direction: they're append-only, essentially.
    public static void performUpdateForwardValidationChecks(JSONObject oldMatchJSON, JSONObject newMatchJSON) throws ValidationException {
        try {
            verifyOptionalArraysEqual(oldMatchJSON, newMatchJSON, "gameRoleNames", false, false, false);
            verifyOptionalArraysEqual(oldMatchJSON, newMatchJSON, "isPlayerHuman", false, false, false);
            verifyOptionalArraysEqual(oldMatchJSON, newMatchJSON, "playerNamesFromHost", false, false, true);
            verifyOptionalArraysEqual(oldMatchJSON, newMatchJSON, "moves", true, false, false);
            verifyOptionalArraysEqual(oldMatchJSON, newMatchJSON, "errors", true, false, false);
            verifyOptionalArraysEqual(oldMatchJSON, newMatchJSON, "states", true, true, false);
            verifyOptionalArraysEqual(oldMatchJSON, newMatchJSON, "stateTimes", true, false, false);
            if (oldMatchJSON.has("isCompleted") && newMatchJSON.has("isCompleted") && oldMatchJSON.getBoolean("isCompleted") && !newMatchJSON.getBoolean("isCompleted")) {
                throw new ValidationException("Cannot transition from completed to not-completed.");
            }
        } catch(JSONException e) {
            throw new ValidationException("Could not parse JSON: " + e.toString());
        }
    }
    
    private static final Set<String> validKeys = new HashSet<String>(Arrays.asList(new String[] {
    		"randomToken", "playerNamesFromHost", "weight", "analysisClock", "moves", "states",
    		"scrambled", "gameName", "goalValues", "isPlayerHuman", "startTime", "playClock",
    		"stateTimes", "gameRoleNames", "tournamentNameFromHost", "errors", "matchHostSignature",
    		"startClock", "matchId", "gameMetaURL", "previewClock", "matchHostPK", "isAborted",
    		"isCompleted" }));    

    public static void performInternalConsistencyChecks(JSONObject theMatchJSON) throws ValidationException {
        try {
        	verifyNoNulls(theMatchJSON, "theMatchJSON");
            verifyHas(theMatchJSON, "matchId");
            verifyHas(theMatchJSON, "startTime");
            verifyHas(theMatchJSON, "randomToken");
            verifyHas(theMatchJSON, "startClock");
            verifyHas(theMatchJSON, "playClock");
            verifyHas(theMatchJSON, "states");
            verifyHas(theMatchJSON, "moves");
            verifyHas(theMatchJSON, "stateTimes");
            verifyHas(theMatchJSON, "gameMetaURL");
            
            Iterator<?> keyIterator = theMatchJSON.keys();
            while (keyIterator.hasNext()) {
            	String key = keyIterator.next().toString();
            	if (!validKeys.contains(key) && !key.startsWith("x-")) {
            		throw new ValidationException("Unexpected key: " + key);
            	}
            }            

            try {
                String theGameURL = theMatchJSON.getString("gameMetaURL");
                String theSuffix = theGameURL.substring(theGameURL.lastIndexOf("/v"));
                Integer.parseInt(theSuffix.substring(2, theSuffix.length()-1));
            } catch (NumberFormatException nfe) {
                throw new ValidationException("gameMetaURL is not properly version-qualified.");
            } catch (IndexOutOfBoundsException ibe) {
                throw new ValidationException("gameMetaURL is not properly version-qualified.");
            }

            int movesLength = theMatchJSON.getJSONArray("moves").length();
            int statesLength = theMatchJSON.getJSONArray("states").length();
            int stateTimesLength = theMatchJSON.getJSONArray("stateTimes").length();
            if (statesLength < 1) {
            	throw new ValidationException("The initial state must be present before a match is published, but here it isn't.");
            }            
            if (statesLength != stateTimesLength) {
                throw new ValidationException("There are " + statesLength + " states, but " + stateTimesLength + " state times. Inconsistent!");
            }
            if (statesLength != movesLength+1) {
                throw new ValidationException("There are " + statesLength + " states, but " + movesLength + " moves. Inconsistent!");
            }
            if (movesLength > 0) {
                int nPlayers = theMatchJSON.getJSONArray("moves").getJSONArray(0).length();
                for (int i = 0; i < movesLength; i++) {
                    if (nPlayers != theMatchJSON.getJSONArray("moves").getJSONArray(i).length()) {
                        throw new ValidationException("Moves array starts with " + nPlayers + " players, but later has " + theMatchJSON.getJSONArray("moves").getJSONArray(i).length() + " moves. Inconsistent!");
                    }
                    verifyStringArray(theMatchJSON.getJSONArray("moves").getJSONArray(i));
                }
                if (theMatchJSON.has("goalValues")) {
                    if (theMatchJSON.has("isCompleted") && !theMatchJSON.getBoolean("isCompleted")) {
                        throw new ValidationException("goalValues is present when isCompleted is false.");
                    }
                    if (theMatchJSON.getJSONArray("goalValues").length() != nPlayers) {
                        throw new ValidationException("Moves array starts with " + nPlayers + " players, but goals array has " + theMatchJSON.getJSONArray("goalValues").length() + " players. Inconsistent!");
                    }
                }
                if (theMatchJSON.has("playerNamesFromHost")) {
                    if (theMatchJSON.getJSONArray("playerNamesFromHost").length() != nPlayers) {
                        throw new ValidationException("Moves array starts with " + nPlayers + " players, but playerNamesFromHost array has " + theMatchJSON.getJSONArray("playerNamesFromHost").length() + " players. Inconsistent!");
                    }
                }
            }
            if (theMatchJSON.has("errors")) {
                JSONArray errors = theMatchJSON.getJSONArray("errors");
                int errorsLength = errors.length();
                if (errorsLength != statesLength) {
                    throw new ValidationException("There are " + statesLength + " states, but " + errorsLength + " error listings. Inconsistent!");
                }
                for (int i = 0; i < errors.length(); i++) {
                    if (errors.getJSONArray(i).length() != errors.getJSONArray(0).length()) {
                        throw new ValidationException("Errors array lengths are inconsistent!");
                    }
                    verifyStringArray(errors.getJSONArray(i));
                }
                if (theMatchJSON.getJSONArray("moves").length() > 0) {
                    int nPlayers = theMatchJSON.getJSONArray("moves").getJSONArray(0).length();
                    if (nPlayers != errors.getJSONArray(0).length()) {
                        throw new ValidationException("Inconsistency between the number of players in moves array, and players in errors array: " + nPlayers + " vs " + errors.getJSONArray(0).length());
                    }
                }
            }

            long theTime = theMatchJSON.getLong("startTime");
            verifyReasonableTime(theMatchJSON.getLong("startTime"));
            for (int i = 0; i < stateTimesLength; i++) {
                verifyReasonableTime(theMatchJSON.getJSONArray("stateTimes").getLong(i));
                if (theTime > theMatchJSON.getJSONArray("stateTimes").getLong(i)) {
                    throw new ValidationException("Time sequence goes backward!");
                } else {
                    theTime = theMatchJSON.getJSONArray("stateTimes").getLong(i);
                }
            }

            if (theMatchJSON.has("matchHostPK")) {
                if (!SignableJSON.isSignedJSON(theMatchJSON)) {
                    throw new ValidationException("Match has a host-PK but is not signed!");
                }
                if (!SignableJSON.verifySignedJSON(theMatchJSON)) {
                    throw new ValidationException("Match has a host-PK and is signed, but signature does not validate!");
                }
            } else {
                if (theMatchJSON.has("matchHostSignature")) {
                    throw new ValidationException("Any match with a signature must also contain a matchHostPK field.");
                }
            }
        } catch(JSONException e) {
            throw new ValidationException("Could not parse JSON: " + e.toString());
        }
    }

    public static void verifyNoNulls(Object o, String objName) throws JSONException, ValidationException {
    	if (o instanceof Boolean) {
    		;
    	} else if (o instanceof Number) {
    		;
    	} else if (o instanceof String) {
    		;
    	} else if (o == JSONObject.NULL) {
    		throw new ValidationException("Found an unexpected null value in field " + objName);
    	} else if (o instanceof JSONObject) {
            for (String key : JSONObject.getNames(o)) {
            	if (key.equals("NULL")) continue;
            	if (((JSONObject) o).isNull(key)) {
            		throw new ValidationException("Found null value for field " + key + " in object " + objName);
            	}
            	verifyNoNulls(((JSONObject) o).get(key), key);
            }
    	} else if (o instanceof JSONArray) {
            for (int i = 0; i < ((JSONArray) o).length(); i++) {
            	if (((JSONArray) o).isNull(i)) {
            		throw new ValidationException("Found null value for element " + i + " in array " + objName);
            	}
            	verifyNoNulls(((JSONArray) o).get(i), objName);
            }
    	}
    }

    public static void verifyReasonableTime(long theTime) throws ValidationException {
        if (theTime < 0) throw new ValidationException("Time is negative!");
        if (theTime < 1200000000000L) throw new ValidationException("Time is before GGP Galaxy began.");
        if (theTime > System.currentTimeMillis() + 604800000L) throw new ValidationException("Time is after a week from now.");
    }

    public static void verifyStringArray(JSONArray arr) throws ValidationException {
    	for (int i = 0; i < arr.length(); i++) {
    		if (arr.optJSONArray(i) != null) {
    			throw new ValidationException("Expecting string in array, but found JSON array: " + arr.optJSONArray(i));
    		}
    		if (arr.optJSONObject(i) != null) {
    			throw new ValidationException("Expecting string in array, but found JSON object: " + arr.optJSONObject(i));
    		}
    		if (arr.isNull(i)) {
    			throw new ValidationException("Expecting string in array, but found null.");
    		}
    	}
    }

    public static void verifyHas(JSONObject obj, String k) throws ValidationException {
        if (!obj.has(k)) {
            throw new ValidationException("Could not find required field " + k);
        }
    }

    public static void verifyEquals(JSONObject old, JSONObject newer, String k) throws JSONException, ValidationException {
        if (!old.has(k) && !newer.has(k)) {
            return;
        } else if (!old.has(k)) {
            throw new ValidationException("Incompability for " + k + ": old has null, new has [" + newer.get(k) + "].");
        } else if (!old.get(k).equals(newer.get(k))) {
            throw new ValidationException("Incompability for " + k + ": old has [" + old.get(k) + "], new has [" + newer.get(k) + "].");
        }
    }

    public static void verifyOptionalArraysEqual(JSONObject old, JSONObject newer, String arr, boolean arrayCanExpand, boolean compareElementsAsSymbolSets, boolean allowEmptyToNonempty) throws JSONException, ValidationException {
        if (!old.has(arr) && !newer.has(arr)) return;
        if (old.has(arr) && !newer.has(arr)) throw new ValidationException("Array " + arr + " missing from new, present in old.");
        if (!old.has(arr) && newer.has(arr)) return; // okay for the array to appear mid-way through the game
        JSONArray oldArr = old.getJSONArray(arr);
        JSONArray newArr = newer.getJSONArray(arr);
        if (!arrayCanExpand && oldArr.length() != newArr.length()) throw new ValidationException("Array " + arr + " has length " + newArr.length() + " in new, length " + oldArr.length() + " in old.");
        if (newArr.length() < oldArr.length()) throw new ValidationException("Array " + arr + " shrank from length " + oldArr.length() + " to length " + newArr.length() + ".");
        for (int i = 0; i < oldArr.length(); i++) {
            if (compareElementsAsSymbolSets) {
                Set<String> oldArrElemSet = verifyOptionalArraysEqual_GenerateSymbolSet(oldArr.get(i).toString());
                Set<String> newArrElemSet = verifyOptionalArraysEqual_GenerateSymbolSet(newArr.get(i).toString());
                if (oldArrElemSet == null) {
                    throw new ValidationException("Cannot parse symbol set in old array " + arr + " at element " + i + ".");
                } else if (newArrElemSet == null) {
                    throw new ValidationException("Cannot parse symbol set in new array " + arr + " at element " + i + ".");
                } else if (!oldArrElemSet.equals(newArrElemSet)) {
                    throw new ValidationException("Array " + arr + " has set-wise disagreement between new [" + newArr.get(i) + "] and old [" + oldArr.get(i) + "] at element " + i + ".");
                }
            } else {
                if (oldArr.get(i) instanceof JSONArray) {
                    if (!(newArr.get(i) instanceof JSONArray))
                        throw new ValidationException("Array " + arr + " used to have interior arrays but no longer does, in position " + i + ".");
                    if(!oldArr.getJSONArray(i).toString().equals(newArr.getJSONArray(i).toString()))
                        throw new ValidationException("Array " + arr + " has internal disagreement between new [" + newArr.get(i) + "] and old [" + oldArr.get(i) + "] at element " + i + ".");
                } else if (!oldArr.get(i).equals(newArr.get(i))) {
                	if (allowEmptyToNonempty && oldArr.get(i).toString().isEmpty() && !newArr.get(i).toString().isEmpty()) {
                		// Value changed, but the old value was empty and the new value isn't, and we're allowing that
                	} else {
                		throw new ValidationException("Array " + arr + " has disagreement between new [" + newArr.get(i) + "] and old [" + oldArr.get(i) + "] at element " + i + ".");
                	}
                }
            }
        }
    }
    public static Set<String> verifyOptionalArraysEqual_GenerateSymbolSet(String x) {
        try {
            Set<String> y = new HashSet<String>();
            SymbolList l = (SymbolList)SymbolFactory.create(x);
            for (int i = 0; i < l.size(); i++) y.add(l.get(i).toString());
            return y;
        } catch (SymbolFormatException q) {
            return null;
        }
    }
}
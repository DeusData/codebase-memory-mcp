FUNCTION F_Clamp : REAL
VAR_INPUT
    rValue : REAL;
    rMin : REAL;
    rMax : REAL;
END_VAR
IF rValue < rMin THEN
    F_Clamp := rMin;
ELSIF rValue > rMax THEN
    F_Clamp := rMax;
ELSE
    F_Clamp := rValue;
END_IF
END_FUNCTION

CREATE OR REPLACE PACKAGE emp_pkg AS
  FUNCTION hire(p_name VARCHAR2) RETURN NUMBER;
END emp_pkg;
/

CREATE OR REPLACE PACKAGE BODY emp_pkg AS
  FUNCTION hire(p_name VARCHAR2) RETURN NUMBER IS
    v_sal NUMBER;
  BEGIN
    v_sal := util_pkg.calc_salary(p_name);
    IF v_sal > 0 THEN
      RETURN v_sal;
    END IF;
    RAISE no_data_found;
  END;
END emp_pkg;
/

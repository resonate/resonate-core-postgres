/***********************************************************************************************************************
* UTILITY Eval
***********************************************************************************************************************/
create or replace function _utility.eval_boolean(strExpression text) returns boolean as $$
DECLARE
    s varchar;
    r boolean;
BEGIN

    s := 'select ' || strExpression ;
    execute s into r;
    return r;

END;
$$ language plpgsql security definer immutable;

do $$ begin execute 'grant execute on function _utility.eval_boolean(text) to ' || _utility.role_get('user'); end $$;
do $$ begin execute 'grant execute on function _utility.eval_boolean(text) to ' || _utility.role_get('etl'); end $$;

    
create or replace function _utility.eval_safe_division(nNumerator numeric, nDenominator numeric, nValueIfDenominatorIsZero numeric default 0) returns numeric as $$
BEGIN
    if (nDenominator = 0) then
        return nValueIfDenominatorIsZero;
    else
        return nNumerator / nDenominator;
    end if;
END;
$$ language plpgsql security definer immutable;

do $$ begin execute 'grant execute on function _utility.eval_safe_division(numeric, numeric, numeric) to ' || _utility.role_get('user'); end $$;
do $$ begin execute 'grant execute on function _utility.eval_safe_division(numeric, numeric, numeric) to ' || _utility.role_get('etl'); end $$;

create or replace function _utility.eval_safe_division(nNumerator int, nDenominator int, nValueIfDenominatorIsZero numeric default 0) returns numeric as $$
BEGIN
    if (nDenominator = 0) then
        return nValueIfDenominatorIsZero;
    else
        return nNumerator::numeric / nDenominator::numeric;
    end if;
END;
$$ language plpgsql security definer immutable;

do $$ begin execute 'grant execute on function _utility.eval_safe_division(int,int, numeric) to ' || _utility.role_get('user'); end $$;
do $$ begin execute 'grant execute on function _utility.eval_safe_division(int,int, numeric) to ' || _utility.role_get('etl'); end $$;

create or replace function _utility.eval_safe_division(nNumerator int, nDenominator numeric, nValueIfDenominatorIsZero numeric default 0) returns numeric as $$
BEGIN
    if (nDenominator = 0) then
        return nValueIfDenominatorIsZero;
    else
        return nNumerator::numeric / nDenominator;
    end if;
END;
$$ language plpgsql security definer immutable;

do $$ begin execute 'grant execute on function _utility.eval_safe_division(int, numeric, numeric) to ' || _utility.role_get('user'); end $$;
do $$ begin execute 'grant execute on function _utility.eval_safe_division(int, numeric, numeric) to ' || _utility.role_get('etl'); end $$;

create or replace function _utility.eval_safe_division(nNumerator numeric, nDenominator int, nValueIfDenominatorIsZero numeric default 0) returns numeric as $$
BEGIN
    if (nDenominator = 0) then
        return nValueIfDenominatorIsZero;
    else
        return nNumerator / nDenominator::numeric;
    end if;
END;
$$ language plpgsql security definer immutable;

do $$ begin execute 'grant execute on function _utility.eval_safe_division(numeric, int, numeric) to ' || _utility.role_get('user'); end $$;
do $$ begin execute 'grant execute on function _utility.eval_safe_division(numeric, int, numeric) to ' || _utility.role_get('etl'); end $$;

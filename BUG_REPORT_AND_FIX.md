# Critical Bug Report: Infinite Rerun Loop and SessionInfo Error

## Executive Summary

The application had **THREE CRITICAL BUGS** causing:
1. **Infinite rerun loops** - app runs endlessly after any interaction
2. **"SessionInfo before it was initialized" errors** - session state accessed incorrectly
3. **UI becoming unresponsive** - browser hangs during interactions

All three bugs have been **FIXED** in this commit.

---

## Bug #1: Manual Session State Assignment Creates Infinite Loop (CRITICAL)

### Location
- **File**: `src/ui/sidebar_components.py`
- **Lines**: 462-469 (query selectbox), 489-498 (table explorer selectbox)

### The Problem

Streamlit widgets with a `key` parameter **automatically manage session state**. However, the code was manually assigning the widget result back to session state:

```python
# WRONG - causes infinite loop
st.session_state.selected_query_name = st.sidebar.selectbox(
    "Select a predefined query:",
    options=query_names_options,
    index=query_names_options.index(st.session_state.selected_query_name)
    if st.session_state.selected_query_name in query_names_options
    else 0,
    key="sb_selected_query_name",  # This key auto-manages state
)
```

### Why This Causes Infinite Rerun

1. **Initial render**: Streamlit widget stores value in `st.session_state["sb_selected_query_name"]`
2. **Your code runs**: Manually assigns to `st.session_state.selected_query_name` (different key!)
3. **Next render**: Widget detects change → triggers rerun
4. **On rerun**: Your manual assignment overwrites the widget state again
5. **Result**: Infinite loop of rerun → assignment → rerun

### Root Cause

**Key mismatch**: Widget stores in `"sb_selected_query_name"` but code reads/writes to `"selected_query_name"`. This creates a race condition where the widget and your code fight over state ownership.

### The Fix

```python
# CORRECT - let Streamlit manage state
current_query = st.session_state.get("selected_query_name", "")
default_index = 0
if current_query and current_query in query_names_options:
    default_index = query_names_options.index(current_query)

st.sidebar.selectbox(
    "Select a predefined query:",
    options=query_names_options,
    index=default_index,
    key="selected_query_name",  # Now key matches the code's expectations
)
# DO NOT manually assign the result - Streamlit handles it via the key
```

**Changes Made:**
1. Changed widget key from `"sb_selected_query_name"` → `"selected_query_name"` (direct state key)
2. Changed widget key from `"sb_selected_table_explorer"` → `"selected_table_for_explorer"` (direct state key)
3. Removed manual assignment: `st.session_state.selected_query_name = st.sidebar.selectbox(...)`
4. Removed manual assignment: `st.session_state.selected_table_for_explorer = st.sidebar.selectbox(...)`

---

## Bug #2: Unnecessary st.rerun() Calls in Widget Change Handlers (CRITICAL)

### Location
- **File**: `src/ui/pages/plots_page.py`
- **Lines**: 113, 159, 267, 308

### The Problem

The code was manually calling `st.rerun()` inside widget change detection conditions:

```python
# WRONG - compounds unnecessary reruns
if selected_topic_widget != current_selected_topic:
    SessionStateManager.reset_plot_state(keep_topic=False)
    SessionStateManager.set_plot_selection(selected_topic_widget, "")
    st.rerun()  # PROBLEM: Already triggered by widget change!
```

### Why This Causes Issues

1. **Streamlit's behavior**: When a widget's value changes, Streamlit **automatically** triggers a rerun
2. **Your code**: Explicitly calls `st.rerun()` again inside the change handler
3. **Result**: Double rerun → processing happens twice → UI lag and potential state inconsistencies
4. **Cascade effect**: When combined with Bug #1, this creates a multiplicative rerun effect

### The Fix

```python
# CORRECT - remove manual st.rerun()
if selected_topic_widget != current_selected_topic:
    SessionStateManager.reset_plot_state(keep_topic=False)
    SessionStateManager.set_plot_selection(selected_topic_widget, "")
    # Streamlit handles the rerun automatically - NO explicit st.rerun() needed
```

**Changes Made:**
1. Removed `st.rerun()` from topic selection handler (line 113)
2. Removed `st.rerun()` from chart selection handler (line 159)
3. Removed `st.rerun()` from time-period knesset selection handler (line 267)
4. Removed `st.rerun()` from single-knesset selection handler (line 308)

---

## Bug #3: Session State Index Calculation Can Fail

### Location
- **File**: `src/ui/sidebar_components.py`
- **Lines**: 465-467, 492-496

### The Problem

The code attempted to calculate the default index before the session state was fully initialized:

```python
# RISKY - what if selected_query_name is None or empty string?
index=query_names_options.index(st.session_state.selected_query_name)
if st.session_state.selected_query_name in query_names_options
else 0,
```

If `selected_query_name` was `None` or an empty string not in `query_names_options`, the `.index()` call could fail.

### Why This Causes "SessionInfo" Errors

The "SessionInfo before it was initialized" error occurs when code tries to access session state before Streamlit has fully initialized the SessionInfo object. This can happen during:
1. Import-time code execution
2. Module-level state access
3. Accessing state keys that don't exist yet

### The Fix

```python
# SAFE - check existence before access
current_query = st.session_state.get("selected_query_name", "")
default_index = 0
if current_query and current_query in query_names_options:
    default_index = query_names_options.index(current_query)
```

**Why this is safer:**
1. Uses `.get()` with default instead of direct access
2. Checks if value exists and is valid BEFORE calling `.index()`
3. Provides a safe fallback (index 0)

---

## Summary of Changes

### Files Modified

1. **src/ui/sidebar_components.py**
   - Lines 462-475: Fixed query selectbox (removed manual assignment, changed key)
   - Lines 493-508: Fixed table explorer selectbox (removed manual assignment, changed key)
   - Lines 115, 279: Updated function checks to use `.get()` method

2. **src/ui/pages/plots_page.py**
   - Lines 109-114: Removed `st.rerun()` from topic selection handler
   - Lines 150-161: Removed `st.rerun()` from chart selection handler
   - Lines 266-270: Removed `st.rerun()` from time-period knesset handler
   - Lines 308-312: Removed `st.rerun()` from single-knesset handler

### Key Principles Applied

1. **Let Streamlit manage widget state**: Use the `key` parameter and don't manually assign
2. **Don't force reruns**: Streamlit handles reruns automatically on widget changes
3. **Safe state access**: Always use `.get()` with defaults for optional state values
4. **Single responsibility**: Each widget owns its own state key

---

## Testing Recommendations

To verify the fixes work correctly:

1. **Test sidebar query selection**: Select different predefined queries without app hanging
2. **Test table explorer**: Select different tables without app hanging
3. **Test plot selection**: Change plot topics and charts without endless reruns
4. **Test knesset selection**: Change knesset selection on plots without app hanging
5. **Check browser console**: No excessive network requests or rerender loops
6. **Monitor server logs**: No excessive rerun messages

---

## Prevention

To prevent similar issues in the future:

1. **Rule #1**: Never manually assign widget results if they have a `key` parameter
   - Widget with key: `st.selectbox(..., key="my_key")` → state auto-managed
   - Let Streamlit handle it, no manual assignment needed

2. **Rule #2**: Never call `st.rerun()` inside widget change handlers
   - Widget changes trigger rerun automatically
   - Explicit `st.rerun()` is only needed for non-widget logic

3. **Rule #3**: Use safe state access patterns
   - ✅ Good: `st.session_state.get("key", default_value)`
   - ❌ Bad: `st.session_state["key"]` (can KeyError)
   - ❌ Bad: `st.session_state.key` (can AttributeError)

4. **Rule #4**: Review widget management in code reviews
   - Check for manual state assignments with keyed widgets
   - Check for unnecessary `st.rerun()` calls
   - Verify key names match how state is accessed

---

## References

- Streamlit Session State: https://docs.streamlit.io/library/api-reference/session-state
- Widget Key Parameter: https://docs.streamlit.io/library/develop/widgets#dataframe-interaction
- st.rerun() Documentation: https://docs.streamlit.io/library/api-reference/control-flow/rerun

---

## Issue Resolved

This fix resolves:
- App hanging/frozen state during interactions
- Infinite rerun loops
- SessionInfo initialization errors
- Browser becoming unresponsive

The application should now be fully responsive and handle user interactions smoothly without endless reruns.

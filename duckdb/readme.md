# Funnel Table

## Base snapshot sales activity 
( weekly, month , quarter)
total record : 23,746

example diagram
## Mermaid 
js based diagram markdown
**StackEdit** an online editor

```mermaid
graph LR
A[raw files] -->B[accumulating transform]
    B --> C[periodic snapshot]
    C -->|One| D[Step Process]
    C -->|Two| E[Sales Pipeline]
```